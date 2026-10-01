// Package api exposes the scheduler over gRPC.
//
// The layer is deliberately thin: it converts wire types to domain types, maps
// domain errors onto gRPC status codes, and does nothing else. All the decisions
// live in internal/scheduler, which keeps them testable without a network.
package api

import (
	"context"
	"errors"
	"time"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxAcquireWait bounds how long a worker's long poll may hold a server stream slot.
const maxAcquireWait = 30 * time.Second

// Server implements both the client-facing and worker-facing services.
type Server struct {
	pb.UnimplementedAtlasServiceServer
	pb.UnimplementedWorkerServiceServer

	sched *scheduler.Scheduler
}

// NewServer wraps a scheduler.
func NewServer(s *scheduler.Scheduler) *Server { return &Server{sched: s} }

// toStatus maps domain errors onto the gRPC codes a client can act on.
//
// The mapping is part of the contract, not an implementation detail:
// RESOURCE_EXHAUSTED means "back off and retry later", FAILED_PRECONDITION means
// "stop, your assumption is wrong", and ALREADY_EXISTS means "you reused a key".
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, scheduler.ErrNotFound), errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, scheduler.ErrStaleAttempt):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, scheduler.ErrUnknownWorker), errors.Is(err, scheduler.ErrStaleGeneration):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}

	var admErr *scheduler.AdmissionError
	if errors.As(err, &admErr) {
		if admErr.Reason == "invalid_request" {
			return status.Error(codes.InvalidArgument, admErr.Error())
		}
		if admErr.Reason == "unschedulable" {
			return status.Error(codes.FailedPrecondition, admErr.Error())
		}
		return status.Error(codes.ResourceExhausted, admErr.Error())
	}

	var conflict *scheduler.IdempotencyConflictError
	if errors.As(err, &conflict) {
		return status.Error(codes.AlreadyExists, conflict.Error())
	}

	var illegal *state.ErrIllegalTransition
	if errors.As(err, &illegal) {
		return status.Error(codes.FailedPrecondition, illegal.Error())
	}

	return status.Error(codes.Internal, err.Error())
}

// ---------------------------------------------------------------- client API

// SubmitJob accepts a job, deduplicating on the idempotency key.
func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	spec := scheduler.SubmitSpec{
		IdempotencyKey:     req.IdempotencyKey,
		ClientID:           req.ClientId,
		Image:              req.Image,
		Command:            req.Command,
		Env:                req.Env,
		Request:            types.Resources{CPUMillis: req.CpuMillis, MemoryBytes: req.MemoryBytes},
		Priority:           req.Priority,
		MaxAttempts:        req.MaxAttempts,
		Timeout:            time.Duration(req.TimeoutSeconds) * time.Second,
		RetryOnProcessExit: req.RetryOnProcessExit,
		RetryOnTimeout:     req.RetryOnTimeout,
	}
	if req.DeadlineUnixNanos > 0 {
		d := fromUnixNanos(req.DeadlineUnixNanos)
		spec.Deadline = &d
	}

	job, dedup, err := s.sched.Submit(ctx, spec)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.SubmitJobResponse{
		JobId:              job.ID,
		State:              jobStateToPB[job.State],
		CreatedAtUnixNanos: unixNanos(job.CreatedAt),
		Deduplicated:       dedup,
	}, nil
}

// GetJob returns one job, optionally with its full attempt history.
func (s *Server) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	job, attempts, err := s.sched.GetJob(ctx, req.JobId, req.IncludeAttempts)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.GetJobResponse{Job: jobToPB(job, attempts)}, nil
}

// ListJobs returns jobs newest first.
func (s *Server) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	f := store.JobFilter{
		ClientID: req.ClientId,
		Limit:    int(req.Limit),
		Offset:   int(req.Offset),
	}
	for _, st := range req.States {
		if s, ok := jobStateFromPB[st]; ok {
			f.States = append(f.States, s)
		}
	}
	if f.Limit <= 0 {
		f.Limit = 100
	}

	jobs, err := s.sched.ListJobs(ctx, f)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &pb.ListJobsResponse{Jobs: make([]*pb.Job, 0, len(jobs))}
	for _, j := range jobs {
		out.Jobs = append(out.Jobs, jobToPB(j, nil))
	}
	return out, nil
}

// CancelJob terminates a job. Canceling an already-terminal job succeeds with
// canceled=false rather than erroring, so a client retry is safe.
func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	st, canceled, err := s.sched.CancelJob(ctx, req.JobId, req.Reason)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.CancelJobResponse{State: jobStateToPB[st], Canceled: canceled}, nil
}

// ListWorkers returns the fleet.
func (s *Server) ListWorkers(ctx context.Context, _ *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	workers, err := s.sched.ListWorkers(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &pb.ListWorkersResponse{Workers: make([]*pb.Worker, 0, len(workers))}
	for _, w := range workers {
		out.Workers = append(out.Workers, workerToPB(w))
	}
	return out, nil
}

// DrainWorker stops new placements on a worker without disturbing its running work.
func (s *Server) DrainWorker(ctx context.Context, req *pb.DrainWorkerRequest) (*pb.DrainWorkerResponse, error) {
	st, err := s.sched.DrainWorker(ctx, req.WorkerId, req.Undrain)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.DrainWorkerResponse{State: workerStateToPB[st]}, nil
}

// GetClusterStatus summarizes occupancy and queue depth.
func (s *Server) GetClusterStatus(_ context.Context, _ *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	st := s.sched.Status()
	return &pb.ClusterStatusResponse{
		WorkersTotal:      int32(st.WorkersTotal),
		WorkersHealthy:    int32(st.WorkersHealthy),
		Capacity:          resourcesToPB(st.Capacity),
		Allocated:         resourcesToPB(st.Allocated),
		JobsQueued:        int32(st.JobsQueued),
		JobsRunning:       int32(st.JobsRunning),
		CpuUtilization:    st.CPUUtilization,
		MemoryUtilization: st.MemUtilization,
		SchedulingPolicy:  st.SchedulingPolicy,
		QueueOrdering:     st.QueueOrdering,
	}, nil
}

// ---------------------------------------------------------------- worker API

// RegisterWorker admits a worker to the fleet.
func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	var capacity types.Resources
	if req.Capacity != nil {
		capacity = types.Resources{CPUMillis: req.Capacity.CpuMillis, MemoryBytes: req.Capacity.MemoryBytes}
	}
	res, err := s.sched.RegisterWorker(ctx, scheduler.RegisterRequest{
		WorkerID: req.WorkerId,
		Hostname: req.Hostname,
		Version:  req.Version,
		Labels:   req.Labels,
		Capacity: capacity,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.RegisterWorkerResponse{
		WorkerId:                 res.WorkerID,
		Generation:               res.Generation,
		HeartbeatIntervalMillis:  res.HeartbeatInterval.Milliseconds(),
		LeaseRenewIntervalMillis: res.LeaseRenewInterval.Milliseconds(),
		LeaseTtlMillis:           res.LeaseTTL.Milliseconds(),
	}, nil
}

// Heartbeat records liveness and returns work the worker must abandon.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	res, err := s.sched.Heartbeat(ctx, req.WorkerId, req.Generation, req.ActiveAttemptIds)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.HeartbeatResponse{
		State:            workerStateToPB[res.State],
		CancelAttemptIds: res.CancelAttemptIDs,
		Draining:         res.Draining,
		MustReregister:   res.MustReregister,
	}, nil
}

// AcquireJob long-polls for assignments.
func (s *Server) AcquireJob(ctx context.Context, req *pb.AcquireJobRequest) (*pb.AcquireJobResponse, error) {
	wait := time.Duration(req.WaitMillis) * time.Millisecond
	if wait > maxAcquireWait {
		wait = maxAcquireWait
	}
	assignments, err := s.sched.AcquireJob(ctx, req.WorkerId, req.Generation, wait)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &pb.AcquireJobResponse{Assignments: make([]*pb.Assignment, 0, len(assignments))}
	for _, a := range assignments {
		out.Assignments = append(out.Assignments, assignmentToPB(a))
	}
	return out, nil
}

// StartJob records that execution began.
func (s *Server) StartJob(ctx context.Context, req *pb.StartJobRequest) (*pb.StartJobResponse, error) {
	res, err := s.sched.StartJob(ctx, refFromPB(req.Ref))
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.StartJobResponse{
		LeaseExpiresAtUnixNanos: unixNanos(res.LeaseExpiresAt),
		AlreadyStarted:          res.AlreadyStarted,
	}, nil
}

// RenewLease extends an attempt's authority.
func (s *Server) RenewLease(ctx context.Context, req *pb.RenewLeaseRequest) (*pb.RenewLeaseResponse, error) {
	res, err := s.sched.RenewLease(ctx, refFromPB(req.Ref))
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.RenewLeaseResponse{
		LeaseExpiresAtUnixNanos: unixNanos(res.LeaseExpiresAt),
		Canceled:                res.Canceled,
	}, nil
}

// CompleteJob records success. It is safe to retry after a lost response.
func (s *Server) CompleteJob(ctx context.Context, req *pb.CompleteJobRequest) (*pb.CompleteJobResponse, error) {
	res, err := s.sched.CompleteJob(ctx, refFromPB(req.Ref), req.ExitCode, req.StdoutTail, req.StderrTail)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.CompleteJobResponse{
		JobState:        jobStateToPB[res.JobState],
		AlreadyRecorded: res.AlreadyRecorded,
	}, nil
}

// FailJob records a failure and applies the retry policy.
func (s *Server) FailJob(ctx context.Context, req *pb.FailJobRequest) (*pb.FailJobResponse, error) {
	var exitCode *int32
	if req.HasExitCode {
		c := req.ExitCode
		exitCode = &c
	}
	class, ok := failureClassFromPB[req.FailureClass]
	if !ok {
		class = state.FailureSystemError
	}
	res, err := s.sched.FailJob(ctx, refFromPB(req.Ref), class, req.Message, exitCode, req.StdoutTail, req.StderrTail)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.FailJobResponse{
		JobState:        jobStateToPB[res.JobState],
		WillRetry:       res.WillRetry,
		AlreadyRecorded: res.AlreadyRecorded,
	}, nil
}

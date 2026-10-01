package api

import (
	"time"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/types"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"
)

// This file is the only place where domain types meet wire types. Keeping the
// conversion in one spot means the scheduler never imports protobuf, so the
// simulator and the unit tests can drive it directly.

var jobStateToPB = map[state.JobState]pb.JobState{
	state.JobSubmitted: pb.JobState_JOB_STATE_SUBMITTED,
	state.JobQueued:    pb.JobState_JOB_STATE_QUEUED,
	state.JobAssigned:  pb.JobState_JOB_STATE_ASSIGNED,
	state.JobRunning:   pb.JobState_JOB_STATE_RUNNING,
	state.JobSucceeded: pb.JobState_JOB_STATE_SUCCEEDED,
	state.JobFailed:    pb.JobState_JOB_STATE_FAILED,
	state.JobCanceled:  pb.JobState_JOB_STATE_CANCELED,
}

var jobStateFromPB = reverseJobStates()

func reverseJobStates() map[pb.JobState]state.JobState {
	m := make(map[pb.JobState]state.JobState, len(jobStateToPB))
	for k, v := range jobStateToPB {
		m[v] = k
	}
	return m
}

var attemptStateToPB = map[state.AttemptState]pb.AttemptState{
	state.AttemptAssigned:  pb.AttemptState_ATTEMPT_STATE_ASSIGNED,
	state.AttemptRunning:   pb.AttemptState_ATTEMPT_STATE_RUNNING,
	state.AttemptSucceeded: pb.AttemptState_ATTEMPT_STATE_SUCCEEDED,
	state.AttemptFailed:    pb.AttemptState_ATTEMPT_STATE_FAILED,
	state.AttemptLost:      pb.AttemptState_ATTEMPT_STATE_LOST,
	state.AttemptCanceled:  pb.AttemptState_ATTEMPT_STATE_CANCELED,
}

var failureClassToPB = map[state.FailureClass]pb.FailureClass{
	state.FailureNone:          pb.FailureClass_FAILURE_CLASS_UNSPECIFIED,
	state.FailureProcessExit:   pb.FailureClass_FAILURE_CLASS_PROCESS_EXIT,
	state.FailureTimeout:       pb.FailureClass_FAILURE_CLASS_TIMEOUT,
	state.FailureWorkerLost:    pb.FailureClass_FAILURE_CLASS_WORKER_LOST,
	state.FailureResourceError: pb.FailureClass_FAILURE_CLASS_RESOURCE_ERROR,
	state.FailureSystemError:   pb.FailureClass_FAILURE_CLASS_SYSTEM_ERROR,
	state.FailureCanceled:      pb.FailureClass_FAILURE_CLASS_CANCELED,
}

var failureClassFromPB = reverseFailureClasses()

func reverseFailureClasses() map[pb.FailureClass]state.FailureClass {
	m := make(map[pb.FailureClass]state.FailureClass, len(failureClassToPB))
	for k, v := range failureClassToPB {
		m[v] = k
	}
	return m
}

var workerStateToPB = map[state.WorkerState]pb.WorkerState{
	state.WorkerHealthy:  pb.WorkerState_WORKER_STATE_HEALTHY,
	state.WorkerSuspect:  pb.WorkerState_WORKER_STATE_SUSPECT,
	state.WorkerDead:     pb.WorkerState_WORKER_STATE_DEAD,
	state.WorkerDraining: pb.WorkerState_WORKER_STATE_DRAINING,
	state.WorkerDrained:  pb.WorkerState_WORKER_STATE_DRAINED,
}

func unixNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func unixNanosPtr(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return unixNanos(*t)
}

func fromUnixNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func resourcesToPB(r types.Resources) *pb.ResourceSpec {
	return &pb.ResourceSpec{CpuMillis: r.CPUMillis, MemoryBytes: r.MemoryBytes}
}

func jobToPB(j *types.Job, attempts []*types.Attempt) *pb.Job {
	out := &pb.Job{
		JobId:               j.ID,
		IdempotencyKey:      j.IdempotencyKey,
		ClientId:            j.ClientID,
		State:               jobStateToPB[j.State],
		Priority:            j.Priority,
		Request:             resourcesToPB(j.Request),
		Image:               j.Image,
		Command:             j.Command,
		MaxAttempts:         j.MaxAttempts,
		AttemptCount:        j.AttemptCount,
		TimeoutSeconds:      int64(j.Timeout / time.Second),
		CreatedAtUnixNanos:  unixNanos(j.CreatedAt),
		UpdatedAtUnixNanos:  unixNanos(j.UpdatedAt),
		EnqueuedAtUnixNanos: unixNanos(j.EnqueuedAt),
		EligibleAtUnixNanos: unixNanos(j.EligibleAt),
		DeadlineUnixNanos:   unixNanosPtr(j.Deadline),
		CurrentAttemptId:    j.CurrentAttemptID,
		FailureClass:        failureClassToPB[j.FailureClass],
		Message:             j.Message,
	}
	if j.ExitCode != nil {
		out.ExitCode, out.HasExitCode = *j.ExitCode, true
	}
	for _, a := range attempts {
		out.Attempts = append(out.Attempts, attemptToPB(a))
	}
	return out
}

func attemptToPB(a *types.Attempt) *pb.Attempt {
	out := &pb.Attempt{
		AttemptId:               a.ID,
		JobId:                   a.JobID,
		AttemptNumber:           a.Number,
		WorkerId:                a.WorkerID,
		State:                   attemptStateToPB[a.State],
		LeaseId:                 a.LeaseID,
		LeaseExpiresAtUnixNanos: unixNanos(a.LeaseExpiresAt),
		CreatedAtUnixNanos:      unixNanos(a.CreatedAt),
		StartedAtUnixNanos:      unixNanosPtr(a.StartedAt),
		FinishedAtUnixNanos:     unixNanosPtr(a.FinishedAt),
		FailureClass:            failureClassToPB[a.FailureClass],
		Message:                 a.Message,
		StdoutTail:              a.StdoutTail,
		StderrTail:              a.StderrTail,
	}
	if a.ExitCode != nil {
		out.ExitCode, out.HasExitCode = *a.ExitCode, true
	}
	return out
}

func workerToPB(info scheduler.WorkerInfo) *pb.Worker {
	w := info.Worker
	return &pb.Worker{
		WorkerId:                 w.ID,
		Hostname:                 w.Hostname,
		Version:                  w.Version,
		Labels:                   w.Labels,
		Capacity:                 resourcesToPB(w.Capacity),
		Allocated:                resourcesToPB(w.Allocated),
		State:                    workerStateToPB[w.State],
		RegisteredAtUnixNanos:    unixNanos(w.RegisteredAt),
		LastHeartbeatAtUnixNanos: unixNanos(w.LastHeartbeatAt),
		Generation:               w.Generation,
		RunningAttempts:          int32(info.Running),
	}
}

func assignmentToPB(a scheduler.Assignment) *pb.Assignment {
	return &pb.Assignment{
		JobId:                   a.JobID,
		AttemptId:               a.AttemptID,
		AttemptNumber:           a.AttemptNumber,
		LeaseId:                 a.LeaseID,
		LeaseExpiresAtUnixNanos: unixNanos(a.LeaseExpiresAt),
		Image:                   a.Image,
		Command:                 a.Command,
		Env:                     a.Env,
		Request:                 resourcesToPB(a.Request),
		TimeoutSeconds:          int64(a.Timeout / time.Second),
	}
}

func refFromPB(r *pb.AttemptRef) scheduler.AttemptRef {
	if r == nil {
		return scheduler.AttemptRef{}
	}
	return scheduler.AttemptRef{
		JobID:     r.JobId,
		AttemptID: r.AttemptId,
		LeaseID:   r.LeaseId,
		WorkerID:  r.WorkerId,
	}
}

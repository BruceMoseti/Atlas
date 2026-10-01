package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// SubmitSpec is a job submission.
type SubmitSpec struct {
	// IdempotencyKey deduplicates submission. When empty, Atlas generates one
	// and the submission is not deduplicated — a client-side retry will create a
	// second job. That is documented rather than papered over.
	IdempotencyKey string
	ClientID       string

	// Image empty means run Command directly as a process on the worker.
	Image   string
	Command []string
	Env     map[string]string

	Request  types.Resources
	Priority int32

	MaxAttempts int32
	Timeout     time.Duration
	Deadline    *time.Time

	RetryOnProcessExit bool
	RetryOnTimeout     bool
}

// specHash canonicalizes the parts of a submission that define what will run, so
// that reusing an idempotency key with different parameters can be detected rather
// than silently ignored.
func (s SubmitSpec) specHash() string {
	keys := make([]string, 0, len(s.Env))
	for k := range s.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(s.Image)
	b.WriteByte(0)
	for _, c := range s.Command {
		b.WriteString(c)
		b.WriteByte(0)
	}
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(s.Env[k])
		b.WriteByte(0)
	}
	b.WriteString(strconv.FormatInt(s.Request.CPUMillis, 10))
	b.WriteByte(0)
	b.WriteString(strconv.FormatInt(s.Request.MemoryBytes, 10))
	b.WriteByte(0)
	b.WriteString(strconv.FormatInt(int64(s.Priority), 10))
	b.WriteByte(0)
	b.WriteString(strconv.FormatInt(int64(s.MaxAttempts), 10))
	b.WriteByte(0)
	b.WriteString(strconv.FormatInt(int64(s.Timeout), 10))

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:16])
}

// Submit accepts a job. It returns the job and whether an existing one was returned
// because the idempotency key had already been used.
//
// Ordering matters here: the deduplication lookup happens before admission control.
// A client retrying a submission it is unsure about must get the same answer it would
// have got the first time, even if the queue has filled up in between. Rejecting the
// retry would leave the client believing the job does not exist when it does.
func (s *Scheduler) Submit(ctx context.Context, spec SubmitSpec) (*types.Job, bool, error) {
	if spec.MaxAttempts <= 0 {
		spec.MaxAttempts = s.cfg.Admission.DefaultMaxAttempts
	}
	if len(spec.Command) == 0 {
		return nil, false, &AdmissionError{Reason: "invalid_request", Message: "command must not be empty"}
	}
	if admErr := s.cfg.Admission.checkRequestSize(spec.Request); admErr != nil {
		s.met.JobsRejected.WithLabelValues(admErr.Reason).Inc()
		return nil, false, admErr
	}

	generatedKey := false
	if spec.IdempotencyKey == "" {
		spec.IdempotencyKey = newID("auto")
		generatedKey = true
	}

	var (
		job  *types.Job
		dedo bool
	)
	err := s.store.Update(ctx, func(tx *store.Tx) error {
		job, dedo = nil, false

		existing, err := tx.GetJobByIdempotencyKey(spec.IdempotencyKey)
		if err == nil {
			if existing.RequestSpecHash != spec.specHash() {
				return &IdempotencyConflictError{Key: spec.IdempotencyKey, ExistingJobID: existing.ID}
			}
			job, dedo = existing, true
			return nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}

		if admErr := s.checkAdmission(tx, spec); admErr != nil {
			return admErr
		}

		now := tx.Now()
		j := &types.Job{
			ID:                 newID("job"),
			IdempotencyKey:     spec.IdempotencyKey,
			ClientID:           spec.ClientID,
			State:              state.JobSubmitted,
			Priority:           spec.Priority,
			Request:            spec.Request,
			Image:              spec.Image,
			Command:            spec.Command,
			Env:                spec.Env,
			MaxAttempts:        spec.MaxAttempts,
			Timeout:            spec.Timeout,
			Deadline:           spec.Deadline,
			RetryOnProcessExit: spec.RetryOnProcessExit,
			RetryOnTimeout:     spec.RetryOnTimeout,
			CreatedAt:          now,
			UpdatedAt:          now,
			EnqueuedAt:         now,
			EligibleAt:         now,
			RequestSpecHash:    spec.specHash(),
		}
		if err := tx.InsertJob(j); err != nil {
			return err
		}
		// SUBMITTED is never observable: the row and its move to QUEUED commit
		// together, so a crash here cannot leave an un-enqueued job behind.
		if err := tx.TransitionJob(j, state.JobQueued, "accepted"); err != nil {
			return err
		}
		job = j
		return nil
	})
	if err != nil {
		var admErr *AdmissionError
		if errors.As(err, &admErr) {
			s.met.JobsRejected.WithLabelValues(admErr.Reason).Inc()
		}
		return nil, false, err
	}

	if dedo {
		s.met.JobsSubmitted.WithLabelValues("true").Inc()
		return job, true, nil
	}

	s.mu.Lock()
	s.queue.Push(queuedJobFrom(job))
	s.mu.Unlock()
	s.signalDispatch()

	s.met.JobsSubmitted.WithLabelValues("false").Inc()
	s.log.Info("job_submitted",
		"job_id", job.ID, "client_id", job.ClientID, "priority", job.Priority,
		"cpu_millis", job.Request.CPUMillis, "memory_bytes", job.Request.MemoryBytes,
		"max_attempts", job.MaxAttempts, "generated_key", generatedKey)
	return job, false, nil
}

// checkAdmission applies the backpressure limits. It runs inside the submission
// transaction so that the queue-depth count it reads is the one the insert will add
// to.
func (s *Scheduler) checkAdmission(tx *store.Tx, spec SubmitSpec) *AdmissionError {
	cfg := s.cfg.Admission

	if cfg.MaxQueueDepth > 0 {
		n, err := tx.CountJobsInStates("", state.JobQueued)
		if err != nil {
			return &AdmissionError{Reason: "internal", Message: err.Error()}
		}
		if n >= cfg.MaxQueueDepth {
			return &AdmissionError{
				Reason:  "queue_full",
				Message: fmt.Sprintf("%d jobs queued, limit is %d", n, cfg.MaxQueueDepth),
			}
		}
	}

	if cfg.MaxInFlightPerClient > 0 && spec.ClientID != "" {
		n, err := tx.CountJobsInStates(spec.ClientID,
			state.JobQueued, state.JobAssigned, state.JobRunning)
		if err != nil {
			return &AdmissionError{Reason: "internal", Message: err.Error()}
		}
		if n >= cfg.MaxInFlightPerClient {
			return &AdmissionError{
				Reason:  "client_quota",
				Message: fmt.Sprintf("client %s has %d jobs in flight, limit is %d", spec.ClientID, n, cfg.MaxInFlightPerClient),
			}
		}
	}

	if cfg.RejectUnschedulable {
		s.mu.Lock()
		fits, haveCandidates := s.couldEverRunLocked(spec.Request)
		s.mu.Unlock()
		if haveCandidates && !fits {
			return &AdmissionError{
				Reason: "unschedulable",
				Message: fmt.Sprintf("no worker has capacity for cpu=%d mem=%d even when idle",
					spec.Request.CPUMillis, spec.Request.MemoryBytes),
			}
		}
	}
	return nil
}

// GetJob loads one job, optionally with its attempt history.
func (s *Scheduler) GetJob(ctx context.Context, id string, withAttempts bool) (*types.Job, []*types.Attempt, error) {
	var (
		job      *types.Job
		attempts []*types.Attempt
	)
	err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if job, err = tx.GetJob(id); err != nil {
			return err
		}
		if withAttempts {
			attempts, err = tx.ListAttemptsForJob(id)
		}
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	return job, attempts, nil
}

// ListJobs returns jobs matching a filter, newest first.
func (s *Scheduler) ListJobs(ctx context.Context, f store.JobFilter) ([]*types.Job, error) {
	var jobs []*types.Job
	err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		jobs, err = tx.ListJobs(f)
		return err
	})
	return jobs, err
}

// CancelJob terminates a job. Canceling an already-terminal job is not an error; it
// reports canceled=false, because a client retrying a cancel it is unsure about
// should not get an error for succeeding twice.
//
// Cancellation does not stop the worker synchronously. The attempt's capacity is
// released immediately and the worker is told to kill the execution on its next
// heartbeat or lease renewal, whichever comes first.
func (s *Scheduler) CancelJob(ctx context.Context, id, reason string) (state.JobState, bool, error) {
	if reason == "" {
		reason = "canceled by client"
	}
	var (
		final       state.JobState
		canceled    bool
		workerID    string
		freedWorker *types.Worker
	)

	err := s.store.Update(ctx, func(tx *store.Tx) error {
		canceled, freedWorker = false, nil
		j, err := tx.GetJob(id)
		if err != nil {
			return err
		}
		final = j.State
		if j.State.IsTerminal() {
			return nil
		}

		if j.CurrentAttemptID != "" {
			a, err := tx.GetAttempt(j.CurrentAttemptID)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
			if a != nil && !a.State.IsTerminal() {
				workerID = a.WorkerID
				a.Message = reason
				if err := tx.FinishAttempt(a, state.AttemptCanceled, reason); err != nil {
					return err
				}
				freedWorker = workerAfterRelease(tx, a.WorkerID)
			}
		}

		j.FailureClass = state.FailureCanceled
		j.Message = reason
		if err := tx.TransitionJob(j, state.JobCanceled, reason); err != nil {
			return err
		}
		if err := tx.SaveJob(j); err != nil {
			return err
		}
		final, canceled = j.State, true
		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return "", false, ErrNotFound
	}
	if err != nil {
		return "", false, err
	}

	if canceled {
		s.mu.Lock()
		s.queue.Remove(id)
		s.mu.Unlock()
		// The transaction already released the attempt's capacity and handed
		// back the row it wrote, so the cache update needs no further reads.
		s.releaseWorkerSlot(workerID, freedWorker)
		s.met.JobsCompleted.WithLabelValues(string(state.JobCanceled), string(state.FailureCanceled)).Inc()
		s.met.AttemptsTotal.WithLabelValues(string(state.AttemptCanceled)).Inc()
		s.log.Info("job_canceled", "job_id", id, "worker_id", workerID, "reason", reason)
		s.signalDispatch()
	}
	return final, canceled, nil
}

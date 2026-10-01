package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// AttemptRef scopes a worker-originated mutation to exactly one attempt and one
// lease. Every worker RPC that can change job state carries one.
//
// This is the mechanism that makes at-least-once execution safe to operate. A worker
// that was partitioned, had its lease reclaimed, and then came back holds a ref that
// no longer matches the job's current attempt, so nothing it says can overwrite the
// result of the attempt that replaced it.
type AttemptRef struct {
	JobID     string
	AttemptID string
	LeaseID   string
	WorkerID  string
}

// RegisterRequest is a worker announcing itself.
type RegisterRequest struct {
	WorkerID string
	Hostname string
	Version  string
	Labels   map[string]string
	Capacity types.Resources
}

// RegisterResponse tells the worker how to behave.
type RegisterResponse struct {
	WorkerID           string
	Generation         int64
	HeartbeatInterval  time.Duration
	LeaseRenewInterval time.Duration
	LeaseTTL           time.Duration
}

// RegisterWorker adds or refreshes a worker.
//
// Re-registration is how a restarted worker rejoins, and it implies something
// important: the new process is definitively not executing anything the old one was.
// So every live attempt on that worker is reclaimed immediately, rather than waiting
// out its lease. Re-registration is strictly better evidence than a lease timeout.
func (s *Scheduler) RegisterWorker(ctx context.Context, req RegisterRequest) (RegisterResponse, error) {
	if req.WorkerID == "" {
		return RegisterResponse{}, errors.New("scheduler: worker_id is required")
	}
	if req.Capacity.CPUMillis <= 0 || req.Capacity.MemoryBytes <= 0 {
		return RegisterResponse{}, fmt.Errorf("scheduler: worker %s declared non-positive capacity (cpu=%d mem=%d)",
			req.WorkerID, req.Capacity.CPUMillis, req.Capacity.MemoryBytes)
	}

	var (
		worker    *types.Worker
		requeued  []*types.Job
		reclaimed int
		fresh     bool
	)

	err := s.store.Update(ctx, func(tx *store.Tx) error {
		requeued, reclaimed, fresh = nil, 0, false

		existing, err := tx.GetWorker(req.WorkerID)
		if errors.Is(err, store.ErrNotFound) {
			fresh = true
			w := &types.Worker{
				ID:              req.WorkerID,
				Hostname:        req.Hostname,
				Version:         req.Version,
				Labels:          req.Labels,
				Capacity:        req.Capacity,
				State:           state.WorkerHealthy,
				RegisteredAt:    tx.Now(),
				LastHeartbeatAt: tx.Now(),
				Generation:      1,
			}
			if err := tx.UpsertWorker(w); err != nil {
				return err
			}
			worker = w
			return nil
		}
		if err != nil {
			return err
		}

		live, err := tx.LiveAttempts(req.WorkerID)
		if err != nil {
			return err
		}
		for _, a := range live {
			j, again, err := s.reclaimAttempt(tx, a, state.FailureWorkerLost,
				fmt.Sprintf("worker %s re-registered; previous incarnation's work is gone", req.WorkerID))
			if err != nil {
				return err
			}
			reclaimed++
			if again {
				requeued = append(requeued, j)
			}
		}

		// reclaimAttempt rewrote the worker row, so re-read it.
		w, err := tx.GetWorker(req.WorkerID)
		if err != nil {
			return err
		}
		w.Hostname = req.Hostname
		w.Version = req.Version
		w.Labels = req.Labels
		w.Capacity = req.Capacity
		w.State = state.WorkerHealthy
		w.LastHeartbeatAt = tx.Now()
		w.Generation = existing.Generation + 1
		if err := tx.SaveWorker(w); err != nil {
			return err
		}
		worker = w
		return nil
	})
	if err != nil {
		return RegisterResponse{}, err
	}

	s.mu.Lock()
	s.addWorkerLocked(worker, 0)
	s.workers[worker.ID].lastHeartbeat = s.now()
	for _, j := range requeued {
		s.queue.Push(queuedJobFrom(j))
	}
	s.mu.Unlock()

	if reclaimed > 0 {
		s.met.AttemptsTotal.WithLabelValues("worker_restarted").Add(float64(reclaimed))
	}
	s.log.Info("worker_registered",
		"worker_id", worker.ID, "generation", worker.Generation, "new", fresh,
		"cpu_millis", worker.Capacity.CPUMillis, "memory_bytes", worker.Capacity.MemoryBytes,
		"attempts_reclaimed", reclaimed)
	s.signalDispatch()
	s.refreshFleetMetrics()

	return RegisterResponse{
		WorkerID:   worker.ID,
		Generation: worker.Generation,
		// Renew comfortably more often than the TTL requires, so a single lost
		// renewal does not cost the worker its lease.
		HeartbeatInterval:  s.cfg.HeartbeatInterval,
		LeaseRenewInterval: s.cfg.LeaseTTL / 4,
		LeaseTTL:           s.cfg.LeaseTTL,
	}, nil
}

// HeartbeatResult tells a worker what to do next.
type HeartbeatResult struct {
	State state.WorkerState
	// CancelAttemptIDs are executions the worker must kill: their leases were
	// reclaimed, the job was canceled, or they belong to a previous generation.
	CancelAttemptIDs []string
	Draining         bool
	MustReregister   bool
}

// Heartbeat records liveness and reconciles the worker's view of its own work with
// the scheduler's.
//
// Heartbeats are not persisted on every call. Freshness only has to survive in
// memory, because recovery marks every worker SUSPECT regardless of what the database
// says. Writing a row per worker per interval would cost an fsync for information
// that a restart deliberately throws away.
func (s *Scheduler) Heartbeat(ctx context.Context, workerID string, generation int64, activeAttemptIDs []string) (HeartbeatResult, error) {
	now := s.now()

	s.mu.Lock()
	ws, ok := s.workers[workerID]
	if !ok {
		s.mu.Unlock()
		return HeartbeatResult{MustReregister: true}, nil
	}
	if generation != 0 && generation != ws.generation {
		s.mu.Unlock()
		return HeartbeatResult{MustReregister: true}, nil
	}

	ws.lastHeartbeat = now
	previous := ws.state
	promoted := false
	if ws.state == state.WorkerSuspect || ws.state == state.WorkerDead {
		ws.state = state.WorkerHealthy
		ws.view.Schedulable = true
		promoted = true
	}
	result := HeartbeatResult{
		State:    ws.state,
		Draining: ws.state == state.WorkerDraining || ws.state == state.WorkerDrained,
	}
	needsPersist := promoted || now.Sub(ws.persistedHeartbeat) > 5*s.cfg.HeartbeatInterval
	if needsPersist {
		ws.persistedHeartbeat = now
	}
	s.mu.Unlock()

	if needsPersist {
		persistState := result.State
		if err := s.store.Update(ctx, func(tx *store.Tx) error {
			w, err := tx.GetWorker(workerID)
			if errors.Is(err, store.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			w.LastHeartbeatAt = now
			// A DRAINING or DRAINED worker keeps that state: operator intent
			// outranks liveness.
			if w.State == state.WorkerSuspect || w.State == state.WorkerDead {
				w.State = persistState
			}
			return tx.SaveWorker(w)
		}); err != nil {
			return HeartbeatResult{}, err
		}
	}
	if promoted {
		s.log.Info("worker_state_changed", "worker_id", workerID,
			"state", string(state.WorkerHealthy), "from", string(previous))
		s.signalDispatch()
	}

	if len(activeAttemptIDs) > 0 {
		cancel, err := s.attemptsToCancel(ctx, workerID, activeAttemptIDs)
		if err != nil {
			return HeartbeatResult{}, err
		}
		result.CancelAttemptIDs = cancel
	}
	return result, nil
}

// attemptsToCancel returns the subset of the worker's claimed executions that it no
// longer owns. This is the reverse direction of stale-attempt protection: rather than
// waiting for the worker to report a result we will reject, we tell it to stop as
// soon as we notice.
func (s *Scheduler) attemptsToCancel(ctx context.Context, workerID string, claimed []string) ([]string, error) {
	var cancel []string
	err := s.store.View(ctx, func(tx *store.Tx) error {
		for _, id := range claimed {
			a, err := tx.GetAttempt(id)
			if errors.Is(err, store.ErrNotFound) {
				cancel = append(cancel, id)
				continue
			}
			if err != nil {
				return err
			}
			if a.WorkerID != workerID || a.State.IsTerminal() {
				cancel = append(cancel, id)
			}
		}
		return nil
	})
	return cancel, err
}

// AcquireJob long-polls for assignments the dispatcher has already committed for this
// worker.
//
// The worker pulls; the scheduler decides. Placement stays centralized and
// comparable, while every connection is opened by the worker, so the scheduler never
// needs a route back into the fleet.
func (s *Scheduler) AcquireJob(ctx context.Context, workerID string, generation int64, wait time.Duration) ([]Assignment, error) {
	s.mu.Lock()
	ws, ok := s.workers[workerID]
	if !ok {
		s.mu.Unlock()
		return nil, ErrUnknownWorker
	}
	if generation != 0 && generation != ws.generation {
		s.mu.Unlock()
		return nil, ErrStaleGeneration
	}
	s.mu.Unlock()

	if out := ws.drain(); len(out) > 0 {
		return out, nil
	}
	if wait <= 0 {
		return nil, nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return ws.drain(), nil
		case <-ws.signal:
			if out := ws.drain(); len(out) > 0 {
				return out, nil
			}
		}
	}
}

// validateRef is the gate every worker-originated mutation passes through.
//
// It returns the job and attempt when the caller still holds authority. It returns
// ErrStaleAttempt when the caller does not, and replay=true when the attempt already
// reached a terminal state through this same caller, which means the RPC is a retry
// after a lost response rather than a stale write.
func validateRef(tx *store.Tx, ref AttemptRef) (j *types.Job, a *types.Attempt, replay bool, err error) {
	a, err = tx.GetAttempt(ref.AttemptID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, false, ErrNotFound
	}
	if err != nil {
		return nil, nil, false, err
	}
	if a.JobID != ref.JobID {
		return nil, nil, false, ErrStaleAttempt
	}
	if a.LeaseID != ref.LeaseID {
		return nil, nil, false, ErrStaleAttempt
	}
	if ref.WorkerID != "" && a.WorkerID != ref.WorkerID {
		return nil, nil, false, ErrStaleAttempt
	}

	j, err = tx.GetJob(a.JobID)
	if err != nil {
		return nil, nil, false, err
	}

	if a.State.IsTerminal() {
		// LOST means Atlas reclaimed the attempt and very likely gave the job to
		// someone else. Anything this caller says about it is stale by
		// definition. The other terminal states are outcomes this caller
		// already reported, so repeating them is an idempotent replay.
		if a.State == state.AttemptLost {
			return j, a, false, ErrStaleAttempt
		}
		return j, a, true, nil
	}

	// A live attempt must still be the one the job recognizes.
	if j.CurrentAttemptID != a.ID {
		return j, a, false, ErrStaleAttempt
	}
	return j, a, false, nil
}

// StartResult is the outcome of StartJob.
type StartResult struct {
	LeaseExpiresAt time.Time
	AlreadyStarted bool
}

// StartJob records that a worker has begun executing an attempt.
func (s *Scheduler) StartJob(ctx context.Context, ref AttemptRef) (StartResult, error) {
	var res StartResult
	err := s.store.Update(ctx, func(tx *store.Tx) error {
		res = StartResult{}
		j, a, replay, err := validateRef(tx, ref)
		if err != nil {
			return err
		}
		if replay || a.State == state.AttemptRunning {
			res = StartResult{LeaseExpiresAt: a.LeaseExpiresAt, AlreadyStarted: true}
			return nil
		}

		now := tx.Now()
		a.StartedAt = &now
		a.LeaseExpiresAt = now.Add(s.cfg.LeaseTTL)
		if err := tx.TransitionAttempt(a, state.AttemptRunning, "worker started execution"); err != nil {
			return err
		}
		if err := tx.SaveAttempt(a); err != nil {
			return err
		}
		if j.State == state.JobAssigned {
			if err := tx.TransitionJob(j, state.JobRunning, "execution started"); err != nil {
				return err
			}
		}
		res.LeaseExpiresAt = a.LeaseExpiresAt
		return nil
	})
	if err != nil {
		s.recordRefError("StartJob", ref, err)
		return StartResult{}, err
	}
	if res.AlreadyStarted {
		s.met.IdempotentReplays.WithLabelValues("StartJob").Inc()
	} else {
		s.log.Info("job_started", "job_id", ref.JobID, "attempt_id", ref.AttemptID,
			"worker_id", ref.WorkerID, "lease_id", ref.LeaseID)
	}
	return res, nil
}

// RenewResult is the outcome of RenewLease.
type RenewResult struct {
	LeaseExpiresAt time.Time
	Canceled       bool
}

// RenewLease extends an attempt's authority. A worker that stops renewing loses its
// assignment after LeaseTTL, whether or not it is still alive.
func (s *Scheduler) RenewLease(ctx context.Context, ref AttemptRef) (RenewResult, error) {
	var res RenewResult
	err := s.store.Update(ctx, func(tx *store.Tx) error {
		res = RenewResult{}
		_, a, replay, err := validateRef(tx, ref)
		if err != nil {
			return err
		}
		if replay {
			// The attempt finished or was canceled; there is nothing left to
			// renew and the worker should stop.
			res = RenewResult{LeaseExpiresAt: a.LeaseExpiresAt, Canceled: a.State == state.AttemptCanceled}
			return nil
		}
		a.LeaseExpiresAt = tx.Now().Add(s.cfg.LeaseTTL)
		if err := tx.SaveAttempt(a); err != nil {
			return err
		}
		res.LeaseExpiresAt = a.LeaseExpiresAt
		return nil
	})
	if err != nil {
		s.recordRefError("RenewLease", ref, err)
		return RenewResult{}, err
	}
	return res, nil
}

// CompleteResult is the outcome of CompleteJob.
type CompleteResult struct {
	JobState        state.JobState
	AlreadyRecorded bool
}

// CompleteJob records a successful execution.
//
// Replaying it is safe: a worker whose response was lost can call again and will be
// told the outcome that is already recorded, rather than producing an error or a
// second state change.
func (s *Scheduler) CompleteJob(ctx context.Context, ref AttemptRef, exitCode int32, stdoutTail, stderrTail string) (CompleteResult, error) {
	var res CompleteResult
	var runtime time.Duration
	var freedWorker *types.Worker

	err := s.store.Update(ctx, func(tx *store.Tx) error {
		res, runtime, freedWorker = CompleteResult{}, 0, nil
		j, a, replay, err := validateRef(tx, ref)
		if err != nil {
			return err
		}
		if replay {
			res = CompleteResult{JobState: j.State, AlreadyRecorded: true}
			return nil
		}

		now := tx.Now()
		a.FinishedAt = &now
		a.ExitCode = &exitCode
		a.StdoutTail, a.StderrTail = stdoutTail, stderrTail
		if a.StartedAt != nil {
			runtime = now.Sub(*a.StartedAt)
		}
		// A completion can arrive without a preceding StartJob: the workload may
		// have finished before that RPC landed, or the RPC may have been lost
		// and the worker chose to run anyway. The execution did happen, so walk
		// the attempt and the job through RUNNING rather than inventing a
		// shortcut edge in the state machine.
		if a.State == state.AttemptAssigned {
			a.StartedAt = &now
			if err := tx.TransitionAttempt(a, state.AttemptRunning, "implicit start on completion"); err != nil {
				return err
			}
		}
		if err := tx.FinishAttempt(a, state.AttemptSucceeded, "worker reported success"); err != nil {
			return err
		}
		// FinishAttempt released this worker's reservation. Capture the row it
		// wrote so the cache can be updated without a second read.
		freedWorker = workerAfterRelease(tx, a.WorkerID)

		j.ExitCode = &exitCode
		j.FailureClass = state.FailureNone
		j.Message = ""
		if j.State == state.JobAssigned {
			if err := tx.TransitionJob(j, state.JobRunning, "implicit start on completion"); err != nil {
				return err
			}
		}
		if err := tx.TransitionJob(j, state.JobSucceeded, "attempt succeeded"); err != nil {
			return err
		}
		if err := tx.SaveJob(j); err != nil {
			return err
		}
		res.JobState = j.State
		return nil
	})
	if err != nil {
		s.recordRefError("CompleteJob", ref, err)
		if errors.Is(err, ErrStaleAttempt) {
			s.noteDuplicateExecution(ctx, ref, exitCode)
		}
		return CompleteResult{}, err
	}

	if res.AlreadyRecorded {
		s.met.IdempotentReplays.WithLabelValues("CompleteJob").Inc()
		return res, nil
	}

	s.releaseWorkerSlot(ref.WorkerID, freedWorker)
	s.met.AttemptsTotal.WithLabelValues(string(state.AttemptSucceeded)).Inc()
	s.met.JobsCompleted.WithLabelValues(string(state.JobSucceeded), "").Inc()
	if runtime > 0 {
		s.met.JobRuntimeSeconds.Observe(runtime.Seconds())
	}
	s.log.Info("job_succeeded",
		"job_id", ref.JobID, "attempt_id", ref.AttemptID, "worker_id", ref.WorkerID,
		"exit_code", exitCode, "runtime_ms", runtime.Milliseconds())
	return res, nil
}

// FailResult is the outcome of FailJob.
type FailResult struct {
	JobState        state.JobState
	WillRetry       bool
	AlreadyRecorded bool
}

// FailJob records a failed execution and applies the retry policy.
func (s *Scheduler) FailJob(ctx context.Context, ref AttemptRef, class state.FailureClass, message string, exitCode *int32, stdoutTail, stderrTail string) (FailResult, error) {
	if class == state.FailureNone {
		class = state.FailureSystemError
	}
	var (
		res         FailResult
		requeued    *types.Job
		freedWorker *types.Worker
	)

	err := s.store.Update(ctx, func(tx *store.Tx) error {
		res, requeued, freedWorker = FailResult{}, nil, nil
		j, a, replay, err := validateRef(tx, ref)
		if err != nil {
			return err
		}
		if replay {
			res = FailResult{JobState: j.State, AlreadyRecorded: true}
			return nil
		}

		now := tx.Now()
		a.FinishedAt = &now
		a.ExitCode = exitCode
		a.FailureClass = class
		a.Message = message
		a.StdoutTail, a.StderrTail = stdoutTail, stderrTail
		if err := tx.FinishAttempt(a, state.AttemptFailed, message); err != nil {
			return err
		}
		freedWorker = workerAfterRelease(tx, a.WorkerID)

		again, err := s.disposeJobAfterAttempt(tx, j, a, class, message, exitCode)
		if err != nil {
			return err
		}
		res = FailResult{JobState: j.State, WillRetry: again}
		if again {
			requeued = j
		}
		return nil
	})
	if err != nil {
		s.recordRefError("FailJob", ref, err)
		return FailResult{}, err
	}

	if res.AlreadyRecorded {
		s.met.IdempotentReplays.WithLabelValues("FailJob").Inc()
		return res, nil
	}

	s.mu.Lock()
	if requeued != nil {
		s.queue.Push(queuedJobFrom(requeued))
	}
	s.mu.Unlock()
	s.releaseWorkerSlot(ref.WorkerID, freedWorker)

	s.met.AttemptsTotal.WithLabelValues(string(state.AttemptFailed)).Inc()
	s.log.Warn("attempt_failed",
		"job_id", ref.JobID, "attempt_id", ref.AttemptID, "worker_id", ref.WorkerID,
		"failure_class", string(class), "will_retry", res.WillRetry, "message", message)
	return res, nil
}

// releaseWorkerSlot updates a worker's cached view after one of its attempts ended.
//
// The caller passes the worker row the transaction already wrote, rather than this
// function re-reading it. Two reasons: a completion no longer costs a second
// database round trip, and the window during which the cache still shows the freed
// capacity shrinks to a mutex acquisition.
//
// That window cannot be closed entirely. Doing so would mean holding the scheduler
// mutex across the commit, which would put disk latency in the dispatcher's path —
// a worse trade. So GetClusterStatus is eventually consistent with the store by
// design, and converges within one reconcile tick at the latest.
func (s *Scheduler) releaseWorkerSlot(workerID string, fresh *types.Worker) {
	if workerID == "" {
		return
	}
	s.mu.Lock()
	if ws, ok := s.workers[workerID]; ok {
		if ws.view.Running > 0 {
			ws.view.Running--
		}
		if fresh != nil {
			s.syncWorkerLocked(fresh)
		}
	}
	s.mu.Unlock()
	s.signalDispatch()
}

// workerAfterRelease reads a worker row inside the transaction that just released
// capacity on it, so the caller can hand the authoritative values straight to the
// in-memory cache. A missing row is not an error: a purged worker has no cache
// entry to update either.
func workerAfterRelease(tx *store.Tx, workerID string) *types.Worker {
	if workerID == "" {
		return nil
	}
	w, err := tx.GetWorker(workerID)
	if err != nil {
		return nil
	}
	return w
}

// recordRefError counts and logs rejected worker RPCs.
func (s *Scheduler) recordRefError(rpc string, ref AttemptRef, err error) {
	if !errors.Is(err, ErrStaleAttempt) {
		return
	}
	s.met.StaleRejections.WithLabelValues(rpc).Inc()
	s.log.Warn("stale_attempt_rejected",
		"rpc", rpc, "job_id", ref.JobID, "attempt_id", ref.AttemptID,
		"worker_id", ref.WorkerID, "lease_id", ref.LeaseID)
}

// noteDuplicateExecution records that a reclaimed attempt later reported success.
//
// This is a directly observed duplicate physical execution: Atlas requeued a job
// whose first execution had in fact completed. Counting it is the honest way to
// present at-least-once semantics — the chaos report publishes the number rather than
// claiming it is zero.
func (s *Scheduler) noteDuplicateExecution(ctx context.Context, ref AttemptRef, exitCode int32) {
	if exitCode != 0 {
		return
	}
	s.met.DuplicateExecs.Inc()
	s.log.Warn("duplicate_execution_detected",
		"job_id", ref.JobID, "attempt_id", ref.AttemptID, "worker_id", ref.WorkerID)

	// Record it on the attempt itself so `atlas get` shows the whole story. The
	// attempt's state is terminal and stays terminal; only the note changes.
	_ = s.store.Update(ctx, func(tx *store.Tx) error {
		a, err := tx.GetAttempt(ref.AttemptID)
		if err != nil {
			return nil
		}
		a.Message = strings.TrimSpace(a.Message + " | " + state.LateReportMarker)
		return tx.SaveAttempt(a)
	})
}

// DrainWorker stops new work being placed on a worker without disturbing what it is
// already running. Draining is the operational primitive behind every planned
// maintenance: you take a machine out of rotation, wait, then touch it.
func (s *Scheduler) DrainWorker(ctx context.Context, workerID string, undrain bool) (state.WorkerState, error) {
	target := state.WorkerDraining
	if undrain {
		target = state.WorkerHealthy
	}

	err := s.store.Update(ctx, func(tx *store.Tx) error {
		w, err := tx.GetWorker(workerID)
		if err != nil {
			return err
		}
		if undrain && w.State != state.WorkerDraining && w.State != state.WorkerDrained {
			target = w.State
			return nil
		}
		w.State = target
		return tx.SaveWorker(w)
	})
	if errors.Is(err, store.ErrNotFound) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	if ws, ok := s.workers[workerID]; ok {
		ws.state = target
		ws.view.Schedulable = target.Schedulable()
		if undrain {
			ws.lastHeartbeat = s.now()
		}
	}
	s.mu.Unlock()

	s.log.Info("worker_drain", "worker_id", workerID, "state", string(target), "undrain", undrain)
	s.signalDispatch()
	return target, nil
}

// WorkerInfo is a fleet member as reported to clients.
type WorkerInfo struct {
	Worker  *types.Worker
	Running int
}

// ListWorkers returns the fleet, merging the durable row with the live attempt count
// the scheduler is tracking in memory.
func (s *Scheduler) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	var rows []*types.Worker
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		rows, err = tx.ListWorkers()
		return err
	}); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]WorkerInfo, 0, len(rows))
	for _, w := range rows {
		info := WorkerInfo{Worker: w}
		if ws, ok := s.workers[w.ID]; ok {
			// In-memory state is fresher than the row, which is only written
			// when something changes.
			w.State = ws.state
			w.LastHeartbeatAt = ws.lastHeartbeat
			info.Running = ws.view.Running
		}
		out = append(out, info)
	}
	return out, nil
}

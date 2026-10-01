package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// reconcileLoop is the failure detector. It does three things on a timer: it ages
// worker health based on heartbeat silence, it reclaims leases nobody renewed, and it
// makes sure the in-memory ready queue still matches the database.
//
// The first two paths converge on reclaimAttempt. They exist separately because they
// answer different questions (see docs/SEMANTICS.md §3): heartbeats tell us whether a
// worker process is alive, and leases tell us whether a specific assignment is still
// owned by the worker holding it.
func (s *Scheduler) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ReconcileInterval)
	defer ticker.Stop()

	resyncEvery := int(time.Second / s.cfg.ReconcileInterval)
	if resyncEvery < 1 {
		resyncEvery = 1
	}
	tick := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		tick++

		if err := s.reconcileWorkerHealth(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("worker_health_reconcile_failed", "error", err)
		}
		if _, err := s.sweepExpiredLeases(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("lease_sweep_failed", "error", err)
		}
		if tick%resyncEvery == 0 {
			if n, err := s.resyncQueue(ctx); err != nil && ctx.Err() == nil {
				s.log.Error("queue_resync_failed", "error", err)
			} else if n > 0 {
				s.log.Warn("queue_resynced", "jobs_restored", n)
			}
		}
		s.refreshFleetMetrics()
	}
}

// resyncQueue re-adds any job the database says is QUEUED but the in-memory queue has
// lost, and reports how many it restored.
//
// The in-memory queue is a cache of the QUEUED rows, and a cache needs reconciling:
// a crash between committing a requeue and pushing it in memory would otherwise
// strand the job forever, which would break invariant I6. The sweep is additive only.
// Removing entries would race with concurrent submissions, and it is unnecessary:
// placement re-validates every job against its row before assigning it.
func (s *Scheduler) resyncQueue(ctx context.Context) (int, error) {
	var dbCount int
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		dbCount, err = tx.CountJobsInStates("", state.JobQueued)
		return err
	}); err != nil {
		return 0, err
	}

	s.mu.Lock()
	memCount := s.queue.Len()
	s.mu.Unlock()
	if dbCount <= memCount {
		return 0, nil
	}

	var queued []*types.Job
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		queued, err = tx.ListJobs(store.JobFilter{States: []state.JobState{state.JobQueued}})
		return err
	}); err != nil {
		return 0, err
	}

	restored := 0
	s.mu.Lock()
	for _, j := range queued {
		if !s.queue.Contains(j.ID) {
			s.queue.Push(queuedJobFrom(j))
			restored++
		}
	}
	s.mu.Unlock()
	if restored > 0 {
		s.signalDispatch()
	}
	return restored, nil
}

// reconcileWorkerHealth ages workers through HEALTHY -> SUSPECT -> DEAD.
//
// Two thresholds rather than one is the whole point. A worker that misses a single
// heartbeat stops receiving new work but keeps what it has; only sustained silence
// costs it its assignments. Collapsing these would turn every GC pause into a round
// of duplicate executions.
func (s *Scheduler) reconcileWorkerHealth(ctx context.Context) error {
	now := s.now()

	type change struct {
		id   string
		to   state.WorkerState
		age  time.Duration
		kill bool
	}
	var changes []change
	var purge []string

	s.mu.Lock()
	for id, ws := range s.workers {
		age := now.Sub(ws.lastHeartbeat)
		switch ws.state {
		case state.WorkerHealthy:
			if age > s.cfg.SuspectAfter {
				changes = append(changes, change{id: id, to: state.WorkerSuspect, age: age})
			}
		case state.WorkerSuspect:
			if age > s.cfg.DeadAfter {
				changes = append(changes, change{id: id, to: state.WorkerDead, age: age, kill: true})
			}
		case state.WorkerDraining:
			if ws.view.Running == 0 {
				changes = append(changes, change{id: id, to: state.WorkerDrained, age: age})
			}
		case state.WorkerDead:
			if s.cfg.WorkerPurgeAfter > 0 && age > s.cfg.WorkerPurgeAfter && ws.view.Running == 0 {
				purge = append(purge, id)
			}
		}
	}
	s.mu.Unlock()

	for _, c := range changes {
		var requeued []*types.Job
		var reclaimed int

		err := s.store.Update(ctx, func(tx *store.Tx) error {
			requeued, reclaimed = nil, 0
			w, err := tx.GetWorker(c.id)
			if errors.Is(err, store.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if c.kill {
				live, err := tx.LiveAttempts(c.id)
				if err != nil {
					return err
				}
				for _, a := range live {
					j, again, err := s.reclaimAttempt(tx, a, state.FailureWorkerLost,
						fmt.Sprintf("worker %s declared dead after %s without a heartbeat",
							c.id, c.age.Round(time.Millisecond)))
					if err != nil {
						return err
					}
					reclaimed++
					if again {
						requeued = append(requeued, j)
					}
				}
				// reclaimAttempt rewrote the worker row to release capacity, so
				// re-read it before changing its state.
				if w, err = tx.GetWorker(c.id); err != nil {
					return err
				}
			}
			w.State = c.to
			return tx.SaveWorker(w)
		})
		if err != nil {
			return err
		}

		s.mu.Lock()
		if ws, ok := s.workers[c.id]; ok {
			ws.state = c.to
			ws.view.Schedulable = c.to.Schedulable()
			if c.kill {
				ws.view.Running = 0
				ws.view.Available = ws.view.Capacity
				ws.mailboxMu.Lock()
				ws.pending = nil
				ws.mailboxMu.Unlock()
			}
		}
		for _, j := range requeued {
			s.queue.Push(queuedJobFrom(j))
		}
		s.mu.Unlock()

		s.log.Warn("worker_state_changed",
			"worker_id", c.id,
			"state", string(c.to),
			"heartbeat_age_ms", c.age.Milliseconds(),
			"attempts_reclaimed", reclaimed)
		if reclaimed > 0 {
			s.met.AttemptsTotal.WithLabelValues("worker_dead").Add(float64(reclaimed))
			s.signalDispatch()
		}
	}

	for _, id := range purge {
		if err := s.store.Update(ctx, func(tx *store.Tx) error { return tx.DeleteWorker(id) }); err != nil {
			return err
		}
		s.mu.Lock()
		s.removeWorkerLocked(id)
		s.mu.Unlock()
		s.log.Info("worker_purged", "worker_id", id)
	}
	return nil
}

// sweepExpiredLeases reclaims every attempt whose lease lapsed, and returns how many.
func (s *Scheduler) sweepExpiredLeases(ctx context.Context) (int, error) {
	now := s.now()
	var (
		reclaimed  int
		recoveries []time.Duration
		perWorker  map[string]int
		touched    []*types.Worker
		requeued   []*types.Job
	)

	err := s.store.Update(ctx, func(tx *store.Tx) error {
		reclaimed, recoveries, requeued, touched = 0, nil, nil, nil
		perWorker = make(map[string]int)

		expired, err := tx.ExpiredAttempts(now, s.cfg.MaxLeaseSweep)
		if err != nil {
			return err
		}
		for _, a := range expired {
			lateBy := tx.Now().Sub(a.LeaseExpiresAt)
			j, again, err := s.reclaimAttempt(tx, a, state.FailureWorkerLost,
				fmt.Sprintf("lease %s expired %s ago", a.LeaseID, lateBy.Round(time.Millisecond)))
			if err != nil {
				return err
			}
			reclaimed++
			recoveries = append(recoveries, lateBy)
			perWorker[a.WorkerID]++
			if again {
				requeued = append(requeued, j)
			}
			s.log.Warn("lease_expired",
				"job_id", a.JobID, "attempt_id", a.ID, "worker_id", a.WorkerID,
				"lease_id", a.LeaseID, "late_by_ms", lateBy.Milliseconds())
		}
		for id := range perWorker {
			w, err := tx.GetWorker(id)
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			touched = append(touched, w)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if reclaimed == 0 {
		return 0, nil
	}

	s.mu.Lock()
	for _, w := range touched {
		s.syncWorkerLocked(w)
		if ws, ok := s.workers[w.ID]; ok {
			ws.view.Running -= perWorker[w.ID]
			if ws.view.Running < 0 {
				ws.view.Running = 0
			}
		}
	}
	for _, j := range requeued {
		s.queue.Push(queuedJobFrom(j))
	}
	s.mu.Unlock()

	s.met.LeaseExpirations.Add(float64(reclaimed))
	s.met.AttemptsTotal.WithLabelValues(string(state.AttemptLost)).Add(float64(reclaimed))
	for _, d := range recoveries {
		s.met.RecoveryLatency.Observe(d.Seconds())
	}
	s.signalDispatch()
	return reclaimed, nil
}

// reclaimAttempt is the single path by which Atlas takes an assignment away from a
// worker. It marks the attempt LOST, releases the capacity it held, and then decides
// the job's fate: another attempt, or permanent failure.
//
// The attempt is LOST rather than FAILED because Atlas genuinely does not know what
// happened. The workload may well have completed. That possibility is exactly what
// makes Atlas at-least-once rather than exactly-once, and it is why a job's side
// effects have to be idempotent.
//
// It returns the job and whether it was requeued, so the caller can restore it to the
// in-memory queue once the transaction commits.
func (s *Scheduler) reclaimAttempt(tx *store.Tx, a *types.Attempt, class state.FailureClass, reason string) (*types.Job, bool, error) {
	j, err := tx.GetJob(a.JobID)
	if err != nil {
		return nil, false, err
	}
	if err := tx.FinishAttempt(a, state.AttemptLost, reason); err != nil {
		return nil, false, err
	}
	again, err := s.disposeJobAfterAttempt(tx, j, a, class, reason, nil)
	return j, again, err
}

// disposeJobAfterAttempt decides what happens to a job once one of its attempts has
// reached a terminal state: requeue with backoff, or fail for good.
//
// exitCode is attached to the job only when it fails permanently, so that `atlas get`
// reports the exit status of the attempt that actually decided the outcome.
func (s *Scheduler) disposeJobAfterAttempt(
	tx *store.Tx, j *types.Job, a *types.Attempt,
	class state.FailureClass, reason string, exitCode *int32,
) (bool, error) {
	if j.State.IsTerminal() {
		// A canceled job's attempt can terminate afterwards. The job's outcome
		// was already decided and must not change (invariant I1).
		return false, nil
	}
	if j.CurrentAttemptID == a.ID {
		j.CurrentAttemptID = ""
	}

	retryable := j.Retry().Retryable(class)
	budget := j.AttemptCount < j.MaxAttempts

	if retryable && budget {
		delay := backoffDelay(s.cfg.RetryBaseDelay, s.cfg.RetryMaxDelay, j.AttemptCount)
		j.EnqueuedAt = tx.Now()
		j.EligibleAt = tx.Now().Add(delay)
		j.FailureClass = class
		j.Message = reason
		if err := tx.TransitionJob(j, state.JobQueued, reason); err != nil {
			return false, err
		}
		if err := tx.SaveJob(j); err != nil {
			return false, err
		}
		s.met.RetriesTotal.WithLabelValues(string(class)).Inc()
		s.log.Info("job_requeued",
			"job_id", j.ID, "attempt_id", a.ID, "attempt", a.Number,
			"failure_class", string(class), "backoff_ms", delay.Milliseconds(),
			"attempts_remaining", j.AttemptsRemaining())
		return true, nil
	}

	j.FailureClass = class
	j.Message = reason
	j.ExitCode = exitCode
	if err := tx.TransitionJob(j, state.JobFailed, reason); err != nil {
		return false, err
	}
	if err := tx.SaveJob(j); err != nil {
		return false, err
	}
	s.met.JobsCompleted.WithLabelValues(string(state.JobFailed), string(class)).Inc()

	why := "not retryable"
	if !budget {
		why = "attempt budget exhausted"
	}
	s.log.Warn("job_failed",
		"job_id", j.ID, "attempt_id", a.ID, "attempt", a.Number,
		"failure_class", string(class), "attempts", j.AttemptCount,
		"max_attempts", j.MaxAttempts, "why", why)
	return false, nil
}

// backoffDelay implements exponential backoff with full jitter:
//
//	delay = rand(0, min(base * 2^(attempt-1), max))
//
// Full jitter rather than plain exponential because the failure that caused the retry
// usually hit many jobs at once. Without jitter they all come back simultaneously and
// recreate the overload that killed them.
func backoffDelay(base, max time.Duration, attempt int32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	window := base
	for i := int32(1); i < attempt && window < max; i++ {
		window *= 2
	}
	if window > max {
		window = max
	}
	if window <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(window) + 1))
}

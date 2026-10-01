package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// dispatchLoop runs placement. It wakes on submissions, completions, and worker
// registrations, and otherwise ticks so that retry backoff and newly freed capacity
// are picked up without an explicit event.
func (s *Scheduler) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.DispatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C:
		}

		// Keep sweeping while progress is being made: one batch may free up
		// nothing, but a batch that places 256 jobs usually means there is more
		// ready work behind it.
		for {
			placed, err := s.dispatchOnce(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Error("dispatch_failed", "error", err)
				break
			}
			if placed < s.cfg.MaxDispatchBatch {
				break
			}
		}
	}
}

// candidate pairs a queued job with the worker the policy chose for it.
type candidate struct {
	job      *QueuedJob
	workerID string
	// reserved is what was optimistically subtracted from the cached view, so
	// that a failed batch can be rolled back exactly.
	reserved types.Resources
}

// dispatchOnce builds a batch of placements against the in-memory fleet and commits
// them in a single transaction.
//
// Batching is not a micro-optimization. Every assignment must be durable before the
// worker is told about it, and durability costs one fsync. Committing N placements
// together turns N fsyncs into one, which is the difference between a few hundred and
// a few thousand assignments per second on ordinary hardware.
func (s *Scheduler) dispatchOnce(ctx context.Context) (int, error) {
	now := s.now()

	s.mu.Lock()
	s.queue.PromoteEligible(now)

	var (
		batch    []candidate
		unplaced []*QueuedJob
	)
	for len(batch) < s.cfg.MaxDispatchBatch {
		qj := s.queue.PopBest(now)
		if qj == nil {
			break
		}
		idx, ok := s.cfg.Policy.Select(qj.Request, s.fleet)
		if !ok {
			// Leave the job queued and look at the next one. This is bounded
			// backfill: a job too large for the current cluster must not stop
			// smaller jobs behind it from running, but we also do not scan the
			// whole queue looking for something that fits.
			unplaced = append(unplaced, qj)
			continue
		}
		// Reserve against the cached view so the rest of this batch does not
		// place work on capacity we have already spoken for. Running is not
		// touched here: it counts committed attempts, and a gauge that briefly
		// reports work that may never be assigned is worse than useless.
		view := s.fleet[idx]
		view.Available = view.Available.Sub(qj.Request)
		batch = append(batch, candidate{job: qj, workerID: view.ID, reserved: qj.Request})
	}
	s.queue.Requeue(unplaced)

	if len(batch) == 0 {
		s.mu.Unlock()
		if len(unplaced) > 0 {
			s.met.DispatchDecisis.WithLabelValues("no_capacity").Add(float64(len(unplaced)))
		}
		return 0, nil
	}
	s.mu.Unlock()

	result, err := s.commitPlacements(ctx, batch)

	s.mu.Lock()
	// Undo every optimistic reservation first, then overwrite the views of the
	// workers the transaction actually touched with their committed rows. Doing
	// it in that order means the cache lands on the truth whether the batch
	// committed, partially applied, or rolled back entirely.
	for _, c := range batch {
		if ws, ok := s.workers[c.workerID]; ok {
			ws.view.Available = ws.view.Available.Add(c.reserved)
		}
	}
	if err != nil {
		jobs := make([]*QueuedJob, 0, len(batch))
		for _, c := range batch {
			jobs = append(jobs, c.job)
		}
		s.queue.Requeue(jobs)
		s.mu.Unlock()
		return 0, err
	}
	for _, w := range result.workers {
		s.syncWorkerLocked(w)
	}
	s.queue.Requeue(result.rejected)
	for _, a := range result.assignments {
		if ws, ok := s.workers[a.workerID]; ok {
			ws.view.Running++
			ws.deliver(a.assignment)
		}
	}
	s.mu.Unlock()

	assignments, rejected := result.assignments, result.rejected
	for _, d := range result.scheduleLatency {
		s.met.ScheduleLatency.Observe(d.Seconds())
	}
	for _, d := range result.firstWait {
		s.met.JobWaitSeconds.Observe(d.Seconds())
	}

	for _, a := range assignments {
		s.log.Info("job_assigned",
			"job_id", a.assignment.JobID,
			"attempt_id", a.assignment.AttemptID,
			"attempt", a.assignment.AttemptNumber,
			"worker_id", a.workerID,
			"lease_id", a.assignment.LeaseID,
			"cpu_millis", a.assignment.Request.CPUMillis,
			"memory_bytes", a.assignment.Request.MemoryBytes)
	}

	s.met.DispatchDecisis.WithLabelValues("assigned").Add(float64(len(assignments)))
	s.met.DispatchDecisis.WithLabelValues("revalidation_failed").Add(float64(len(rejected)))
	s.met.AttemptsTotal.WithLabelValues("created").Add(float64(len(assignments)))
	s.refreshFleetMetrics()

	return len(assignments), nil
}

type deliverable struct {
	workerID   string
	assignment Assignment
}

// placementResult is what one committed dispatch batch produced.
type placementResult struct {
	assignments []deliverable
	// rejected jobs failed re-validation and go back on the queue.
	rejected []*QueuedJob
	// workers are the authoritative rows the transaction wrote.
	workers []*types.Worker
	// Latencies are collected rather than observed inside the transaction, so a
	// rollback cannot leave phantom observations behind.
	scheduleLatency []time.Duration
	firstWait       []time.Duration
}

// commitPlacements durably records a batch of assignments.
//
// Each placement is re-validated here against authoritative rows, because the view
// the policy used is a cache. A placement that no longer holds (the job was canceled,
// the worker went SUSPECT, someone else took the capacity) is skipped and its job
// returned to the queue, while the rest of the batch still commits.
func (s *Scheduler) commitPlacements(ctx context.Context, batch []candidate) (placementResult, error) {
	var res placementResult

	err := s.store.Update(ctx, func(tx *store.Tx) error {
		res = placementResult{}
		workers := make(map[string]*types.Worker, len(batch))

		getWorker := func(id string) (*types.Worker, error) {
			if w, ok := workers[id]; ok {
				return w, nil
			}
			w, err := tx.GetWorker(id)
			if err != nil {
				return nil, err
			}
			workers[id] = w
			return w, nil
		}

		for _, c := range batch {
			j, err := tx.GetJob(c.job.JobID)
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if j.State != state.JobQueued {
				// Canceled, or already placed by an earlier batch.
				continue
			}
			if j.EligibleAt.After(tx.Now()) {
				res.rejected = append(res.rejected, c.job)
				continue
			}
			if j.AttemptCount >= j.MaxAttempts {
				// Invariant I5 says this cannot be dispatched. Failing it here
				// rather than looping is the only way out that keeps the budget
				// meaningful.
				j.FailureClass = state.FailureSystemError
				j.Message = "attempt budget exhausted while queued"
				if err := tx.TransitionJob(j, state.JobFailed, j.Message); err != nil {
					return err
				}
				if err := tx.SaveJob(j); err != nil {
					return err
				}
				continue
			}

			w, err := getWorker(c.workerID)
			if errors.Is(err, store.ErrNotFound) {
				res.rejected = append(res.rejected, c.job)
				continue
			}
			if err != nil {
				return err
			}
			if !w.State.Schedulable() || !j.Request.Fits(w.Available()) {
				res.rejected = append(res.rejected, c.job)
				continue
			}

			attempt := &types.Attempt{
				ID:             newID("att"),
				JobID:          j.ID,
				Number:         j.AttemptCount + 1,
				WorkerID:       w.ID,
				State:          state.AttemptAssigned,
				LeaseID:        newID("lse"),
				LeaseExpiresAt: tx.Now().Add(s.cfg.LeaseTTL),
				Request:        j.Request,
				CreatedAt:      tx.Now(),
			}
			if err := tx.InsertAttempt(attempt); err != nil {
				return err
			}

			j.AttemptCount++
			j.CurrentAttemptID = attempt.ID
			if err := tx.TransitionJob(j, state.JobAssigned, "assigned to "+w.ID); err != nil {
				return err
			}
			if err := tx.SaveJob(j); err != nil {
				return err
			}

			w.Allocated = w.Allocated.Add(j.Request)
			if err := tx.SaveWorker(w); err != nil {
				return err
			}

			res.assignments = append(res.assignments, deliverable{
				workerID: w.ID,
				assignment: Assignment{
					JobID:          j.ID,
					AttemptID:      attempt.ID,
					AttemptNumber:  attempt.Number,
					LeaseID:        attempt.LeaseID,
					LeaseExpiresAt: attempt.LeaseExpiresAt,
					Image:          j.Image,
					Command:        j.Command,
					Env:            j.Env,
					Request:        j.Request,
					Timeout:        j.Timeout,
				},
			})

			dispatchable := j.EnqueuedAt
			if j.EligibleAt.After(dispatchable) {
				dispatchable = j.EligibleAt
			}
			res.scheduleLatency = append(res.scheduleLatency, tx.Now().Sub(dispatchable))
			if attempt.Number == 1 {
				res.firstWait = append(res.firstWait, tx.Now().Sub(j.CreatedAt))
			}
		}

		for _, w := range workers {
			res.workers = append(res.workers, w)
		}
		return nil
	})
	if err != nil {
		return placementResult{}, err
	}
	return res, nil
}

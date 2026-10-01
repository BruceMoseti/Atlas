package invariants

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// An invariant checker that cannot fail proves nothing. Every test here builds a
// database containing one specific violation and asserts that the checker names it,
// which is the only way to know a clean chaos report means anything.

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "atlas.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

type fixture struct {
	t  *testing.T
	st *store.Store
}

func newFixture(t *testing.T) *fixture { return &fixture{t: t, st: openStore(t)} }

func (f *fixture) update(fn func(tx *store.Tx) error) {
	f.t.Helper()
	if err := f.st.Update(context.Background(), fn); err != nil {
		f.t.Fatalf("update: %v", err)
	}
}

func (f *fixture) check() *Report {
	f.t.Helper()
	rep, err := Check(context.Background(), f.st)
	if err != nil {
		f.t.Fatalf("check: %v", err)
	}
	return rep
}

func (f *fixture) addWorker(id string, cpu, mem int64) {
	f.update(func(tx *store.Tx) error {
		return tx.UpsertWorker(&types.Worker{
			ID:              id,
			Capacity:        types.Resources{CPUMillis: cpu, MemoryBytes: mem},
			State:           state.WorkerHealthy,
			RegisteredAt:    tx.Now(),
			LastHeartbeatAt: tx.Now(),
			Generation:      1,
		})
	})
}

// addJob creates a QUEUED job.
func (f *fixture) addJob(id string, cpu, mem int64, maxAttempts int32) {
	f.update(func(tx *store.Tx) error {
		j := &types.Job{
			ID: id, IdempotencyKey: "key-" + id, State: state.JobSubmitted,
			Request:     types.Resources{CPUMillis: cpu, MemoryBytes: mem},
			MaxAttempts: maxAttempts, Command: []string{"true"},
			CreatedAt: tx.Now(), UpdatedAt: tx.Now(),
			EnqueuedAt: tx.Now(), EligibleAt: tx.Now(),
		}
		if err := tx.InsertJob(j); err != nil {
			return err
		}
		return tx.TransitionJob(j, state.JobQueued, "accepted")
	})
}

// assign walks a job through a full, correct assignment, reserving capacity.
func (f *fixture) assign(jobID, workerID, attemptID string) {
	f.update(func(tx *store.Tx) error {
		j, err := tx.GetJob(jobID)
		if err != nil {
			return err
		}
		a := &types.Attempt{
			ID: attemptID, JobID: jobID, Number: j.AttemptCount + 1, WorkerID: workerID,
			State: state.AttemptAssigned, LeaseID: "lse-" + attemptID,
			LeaseExpiresAt: tx.Now().Add(time.Minute), Request: j.Request, CreatedAt: tx.Now(),
		}
		if err := tx.InsertAttempt(a); err != nil {
			return err
		}
		j.AttemptCount++
		j.CurrentAttemptID = a.ID
		if err := tx.TransitionJob(j, state.JobAssigned, "assigned"); err != nil {
			return err
		}
		if err := tx.SaveJob(j); err != nil {
			return err
		}
		w, err := tx.GetWorker(workerID)
		if err != nil {
			return err
		}
		w.Allocated = w.Allocated.Add(j.Request)
		return tx.SaveWorker(w)
	})
}

func (f *fixture) succeed(jobID, attemptID string) {
	f.update(func(tx *store.Tx) error {
		a, err := tx.GetAttempt(attemptID)
		if err != nil {
			return err
		}
		if err := tx.TransitionAttempt(a, state.AttemptRunning, "started"); err != nil {
			return err
		}
		if err := tx.FinishAttempt(a, state.AttemptSucceeded, "done"); err != nil {
			return err
		}
		j, err := tx.GetJob(jobID)
		if err != nil {
			return err
		}
		if err := tx.TransitionJob(j, state.JobRunning, "started"); err != nil {
			return err
		}
		if err := tx.TransitionJob(j, state.JobSucceeded, "done"); err != nil {
			return err
		}
		return tx.SaveJob(j)
	})
}

func assertViolates(t *testing.T, rep *Report, invariant string) {
	t.Helper()
	for _, v := range rep.Violations {
		if v.Invariant == invariant {
			return
		}
	}
	t.Fatalf("expected a %s violation, got:\n%s", invariant, rep)
}

func assertClean(t *testing.T, rep *Report) {
	t.Helper()
	if !rep.OK() {
		t.Fatalf("expected no violations, got:\n%s", rep)
	}
}

func TestCleanRunHasNoViolations(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 4000, 4<<30)
	for _, id := range []string{"j1", "j2", "j3"} {
		f.addJob(id, 1000, 1<<30, 3)
		f.assign(id, "w1", "a-"+id)
		f.succeed(id, "a-"+id)
	}

	rep := f.check()
	assertClean(t, rep)
	if rep.JobsSucceeded != 3 || rep.AttemptsSucceeded != 3 {
		t.Fatalf("expected 3 succeeded jobs and attempts, got %d and %d", rep.JobsSucceeded, rep.AttemptsSucceeded)
	}
}

// I2: the store refuses an oversubscription through SaveWorker, so the only way to
// produce one is to write the row behind its back. That is exactly what the checker
// is for: catching corruption the write path did not create.
func TestDetectsOversubscribedWorker(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 1000, 1<<30)
	f.addJob("j1", 1000, 1<<30, 3)
	f.assign("j1", "w1", "a1")

	f.update(func(tx *store.Tx) error {
		return rawExec(tx, `UPDATE workers SET cpu_allocated = 9999 WHERE worker_id = 'w1'`)
	})
	assertViolates(t, f.check(), "I2")
}

func TestDetectsNegativeAllocation(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 4000, 4<<30)
	f.update(func(tx *store.Tx) error {
		return rawExec(tx, `UPDATE workers SET cpu_allocated = -500 WHERE worker_id = 'w1'`)
	})
	assertViolates(t, f.check(), "I2")
}

// I3: the denormalized allocation exists for speed; this is the check that keeps it
// honest.
func TestDetectsAllocationDriftFromLiveAttempts(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 8000, 8<<30)
	f.addJob("j1", 1000, 1<<30, 3)
	f.assign("j1", "w1", "a1")

	f.update(func(tx *store.Tx) error {
		return rawExec(tx, `UPDATE workers SET cpu_allocated = 2000 WHERE worker_id = 'w1'`)
	})
	assertViolates(t, f.check(), "I3")
}

// I4: two live attempts means two workers are running the same job with equal
// authority, which is the failure the lease design exists to prevent.
func TestDetectsTwoLiveAttemptsOnOneJob(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 8000, 8<<30)
	f.addWorker("w2", 8000, 8<<30)
	f.addJob("j1", 1000, 1<<30, 3)
	f.assign("j1", "w1", "a1")

	// The state machine refuses a second ASSIGNED transition, so the only way
	// to reach this state is to write the row directly — which is the point:
	// the checker has to catch what the write path would never create.
	f.update(func(tx *store.Tx) error {
		if err := rawExec(tx, `INSERT INTO attempts
			(attempt_id, job_id, attempt_number, worker_id, state, lease_id, lease_expires_at,
			 cpu_millis, memory_bytes, created_at)
			VALUES ('a2', 'j1', 2, 'w2', 'RUNNING', 'lse-a2', 99999999999999, 1000, 1073741824, 1)`); err != nil {
			return err
		}
		if err := rawExec(tx, `UPDATE jobs SET attempt_count = 2 WHERE job_id = 'j1'`); err != nil {
			return err
		}
		return rawExec(tx, `UPDATE workers SET cpu_allocated = 1000, memory_allocated = 1073741824 WHERE worker_id = 'w2'`)
	})

	assertViolates(t, f.check(), "I4")
}

// I5: an attempt budget that is not enforced is not a budget.
func TestDetectsAttemptBudgetOverrun(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 8000, 8<<30)
	f.addJob("j1", 1000, 1<<30, 1)
	f.assign("j1", "w1", "a1")

	f.update(func(tx *store.Tx) error {
		return rawExec(tx, `UPDATE jobs SET attempt_count = 5 WHERE job_id = 'j1'`)
	})
	assertViolates(t, f.check(), "I5")
}

// I6: a job acknowledged to a client that is no longer in the store is the worst
// failure Atlas could have, because the client has no way to find out.
func TestDetectsVanishedAcceptedJob(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 8000, 8<<30)
	f.addJob("j1", 1000, 1<<30, 3)

	missing, err := CheckAcceptedJobs(context.Background(), f.st, []string{"j1", "never-existed"})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0].Subject != "never-existed" {
		t.Fatalf("expected exactly one I6 violation for the missing job, got %v", missing)
	}
}

// I1: the audit log is what makes this checkable over a whole run. A job that went
// SUCCEEDED and then RUNNING again looks fine in its final row.
func TestDetectsResurrectedTerminalState(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 8000, 8<<30)
	f.addJob("j1", 1000, 1<<30, 3)
	f.assign("j1", "w1", "a1")
	f.succeed("j1", "a1")

	f.update(func(tx *store.Tx) error {
		return rawExec(tx, `INSERT INTO transitions (kind, job_id, attempt_id, from_state, to_state, reason, at)
			VALUES ('job', 'j1', 'a1', 'SUCCEEDED', 'RUNNING', 'a bug', 0)`)
	})
	assertViolates(t, f.check(), "I1")
}

// I8: a job whose outcome came from an attempt that was not its last one is the
// signature of a stale write winning.
func TestDetectsStaleAttemptDecidingTheOutcome(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 8000, 8<<30)
	f.addJob("j1", 1000, 1<<30, 3)
	f.assign("j1", "w1", "a1")
	f.succeed("j1", "a1")

	// A second attempt exists and failed, but the job still reports success
	// from the first. That can only happen if a stale report won.
	f.update(func(tx *store.Tx) error {
		return rawExec(tx, `INSERT INTO attempts
			(attempt_id, job_id, attempt_number, worker_id, state, lease_id, lease_expires_at,
			 cpu_millis, memory_bytes, created_at, finished_at)
			VALUES ('a2', 'j1', 2, 'w1', 'FAILED', 'lse-a2', 0, 1000, 1073741824, 1, 2)`)
	})
	f.update(func(tx *store.Tx) error {
		return rawExec(tx, `UPDATE jobs SET attempt_count = 2 WHERE job_id = 'j1'`)
	})
	assertViolates(t, f.check(), "I8")
}

// I9: a job and its current attempt disagreeing means one of the two is lying about
// what the cluster is doing.
func TestDetectsJobAndAttemptStateDisagreement(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 8000, 8<<30)
	f.addJob("j1", 1000, 1<<30, 3)
	f.assign("j1", "w1", "a1")
	f.succeed("j1", "a1")

	f.update(func(tx *store.Tx) error {
		return rawExec(tx, `UPDATE attempts SET state = 'FAILED' WHERE attempt_id = 'a1'`)
	})
	assertViolates(t, f.check(), "I9")
}

func TestCountsDuplicateExecutions(t *testing.T) {
	f := newFixture(t)
	f.addWorker("w1", 8000, 8<<30)
	f.addJob("j1", 1000, 1<<30, 3)
	f.assign("j1", "w1", "a1")

	// Reclaim the attempt, then record that its worker later reported success:
	// a directly observed duplicate physical execution.
	f.update(func(tx *store.Tx) error {
		a, err := tx.GetAttempt("a1")
		if err != nil {
			return err
		}
		a.Message = state.LateReportMarker
		if err := tx.FinishAttempt(a, state.AttemptLost, "lease expired"); err != nil {
			return err
		}
		j, err := tx.GetJob("j1")
		if err != nil {
			return err
		}
		j.CurrentAttemptID = ""
		if err := tx.TransitionJob(j, state.JobQueued, "requeued"); err != nil {
			return err
		}
		return tx.SaveJob(j)
	})

	rep := f.check()
	assertClean(t, rep)
	if rep.DuplicateExecutions != 1 {
		t.Fatalf("DuplicateExecutions = %d, want 1", rep.DuplicateExecutions)
	}
	if !strings.Contains(rep.String(), "duplicate executions  1") {
		t.Errorf("the report should publish the duplicate count:\n%s", rep)
	}
}

// rawExec runs SQL that the store's own API deliberately refuses, so that the
// checker can be tested against states a correct scheduler cannot produce.
func rawExec(tx *store.Tx, query string) error {
	return tx.ExecRawForTesting(query)
}

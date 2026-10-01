package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/types"
)

func open(t *testing.T) *Store {
	t.Helper()
	st, err := Open(Options{Path: filepath.Join(t.TempDir(), "atlas.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func newJob(id, key string, cpu, mem int64) *types.Job {
	now := time.Now().UTC()
	return &types.Job{
		ID:             id,
		IdempotencyKey: key,
		State:          state.JobSubmitted,
		Request:        types.Resources{CPUMillis: cpu, MemoryBytes: mem},
		MaxAttempts:    3,
		Command:        []string{"echo", "hi"},
		CreatedAt:      now,
		UpdatedAt:      now,
		EnqueuedAt:     now,
		EligibleAt:     now,
	}
}

func newWorker(id string, cpu, mem int64) *types.Worker {
	now := time.Now().UTC()
	return &types.Worker{
		ID:              id,
		Capacity:        types.Resources{CPUMillis: cpu, MemoryBytes: mem},
		State:           state.WorkerHealthy,
		RegisteredAt:    now,
		LastHeartbeatAt: now,
		Generation:      1,
	}
}

func TestInMemoryPathIsRejected(t *testing.T) {
	// The read and write handles would open different databases, which is a
	// confusing failure to debug later. Better to refuse up front.
	if _, err := Open(Options{Path: ":memory:"}); err == nil {
		t.Fatal("an in-memory path should be rejected")
	}
}

func TestJobRoundTrip(t *testing.T) {
	st := open(t)
	ctx := context.Background()

	want := newJob("job_1", "key_1", 2000, 4<<30)
	want.Priority = 7
	want.Env = map[string]string{"FOO": "bar"}
	want.Timeout = 90 * time.Second
	deadline := time.Now().Add(time.Hour).UTC()
	want.Deadline = &deadline

	if err := st.Update(ctx, func(tx *Tx) error {
		if err := tx.InsertJob(want); err != nil {
			return err
		}
		return tx.TransitionJob(want, state.JobQueued, "accepted")
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var got *types.Job
	if err := st.View(ctx, func(tx *Tx) error {
		var err error
		got, err = tx.GetJob("job_1")
		return err
	}); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.State != state.JobQueued {
		t.Errorf("state = %s, want QUEUED", got.State)
	}
	if got.Priority != 7 || got.Request.CPUMillis != 2000 || got.Request.MemoryBytes != 4<<30 {
		t.Errorf("scalar fields did not round-trip: %+v", got)
	}
	if len(got.Command) != 2 || got.Command[0] != "echo" {
		t.Errorf("command = %v, want [echo hi]", got.Command)
	}
	if got.Env["FOO"] != "bar" {
		t.Errorf("env = %v, want FOO=bar", got.Env)
	}
	if got.Timeout != 90*time.Second {
		t.Errorf("timeout = %v, want 90s", got.Timeout)
	}
	if got.Deadline == nil || !got.Deadline.Equal(deadline) {
		t.Errorf("deadline = %v, want %v", got.Deadline, deadline)
	}
}

// TestIdempotencyKeyIsUnique is invariant I7 enforced by the database rather than by
// application logic, so a race between two concurrent submissions cannot defeat it.
func TestIdempotencyKeyIsUnique(t *testing.T) {
	st := open(t)
	ctx := context.Background()

	insert := func(id string) error {
		return st.Update(ctx, func(tx *Tx) error {
			j := newJob(id, "shared-key", 1000, 1<<30)
			if err := tx.InsertJob(j); err != nil {
				return err
			}
			return tx.TransitionJob(j, state.JobQueued, "accepted")
		})
	}
	if err := insert("job_1"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := insert("job_2"); err == nil {
		t.Fatal("a second job reused an idempotency key without an error")
	}

	var n int
	if err := st.View(ctx, func(tx *Tx) error {
		var err error
		n, err = tx.CountJobsInStates("", state.JobQueued)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("%d jobs queued, want 1: the failed insert must have rolled back", n)
	}
}

// TestIllegalTransitionIsRejectedAndRollsBack checks that an invalid state change
// aborts the whole transaction rather than leaving a partial write.
func TestIllegalTransitionIsRejectedAndRollsBack(t *testing.T) {
	st := open(t)
	ctx := context.Background()

	j := newJob("job_1", "key_1", 1000, 1<<30)
	mustUpdate(t, st, func(tx *Tx) error {
		if err := tx.InsertJob(j); err != nil {
			return err
		}
		return tx.TransitionJob(j, state.JobQueued, "accepted")
	})
	mustUpdate(t, st, func(tx *Tx) error {
		if err := tx.TransitionJob(j, state.JobAssigned, "assigned"); err != nil {
			return err
		}
		if err := tx.TransitionJob(j, state.JobRunning, "started"); err != nil {
			return err
		}
		return tx.TransitionJob(j, state.JobSucceeded, "done")
	})

	err := st.Update(ctx, func(tx *Tx) error {
		loaded, err := tx.GetJob("job_1")
		if err != nil {
			return err
		}
		loaded.Priority = 99
		if err := tx.SaveJob(loaded); err != nil {
			return err
		}
		// Terminal states are absorbing. This must fail, and it must take the
		// priority write down with it.
		return tx.TransitionJob(loaded, state.JobRunning, "resurrect")
	})
	var illegal *state.ErrIllegalTransition
	if !errors.As(err, &illegal) {
		t.Fatalf("err = %v, want an illegal-transition error", err)
	}

	var got *types.Job
	mustView(t, st, func(tx *Tx) error {
		var err error
		got, err = tx.GetJob("job_1")
		return err
	})
	if got.State != state.JobSucceeded {
		t.Errorf("state = %s, want SUCCEEDED", got.State)
	}
	if got.Priority == 99 {
		t.Error("the priority write survived a rolled-back transaction")
	}
}

// TestCapacityIsEnforcedByTheStore is invariant I2. Enforcing it in SaveWorker means
// no scheduler bug can oversubscribe a worker: the transaction simply fails.
func TestCapacityIsEnforcedByTheStore(t *testing.T) {
	st := open(t)
	ctx := context.Background()

	w := newWorker("w1", 4000, 8<<30)
	mustUpdate(t, st, func(tx *Tx) error { return tx.UpsertWorker(w) })

	err := st.Update(ctx, func(tx *Tx) error {
		loaded, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		loaded.Allocated = types.Resources{CPUMillis: 5000, MemoryBytes: 1 << 30}
		return tx.SaveWorker(loaded)
	})
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("err = %v, want ErrCapacityExceeded", err)
	}

	err = st.Update(ctx, func(tx *Tx) error {
		loaded, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		loaded.Allocated = types.Resources{CPUMillis: -1, MemoryBytes: 0}
		return tx.SaveWorker(loaded)
	})
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("negative allocation: err = %v, want ErrCapacityExceeded", err)
	}
}

// TestAssignmentIsAtomic is the scenario from docs/SEMANTICS.md: creating the
// attempt, moving the job, and reserving capacity either all happen or none do.
func TestAssignmentIsAtomic(t *testing.T) {
	st := open(t)
	ctx := context.Background()

	j := newJob("job_1", "key_1", 2000, 2<<30)
	w := newWorker("w1", 2000, 2<<30)
	mustUpdate(t, st, func(tx *Tx) error {
		if err := tx.InsertJob(j); err != nil {
			return err
		}
		if err := tx.TransitionJob(j, state.JobQueued, "accepted"); err != nil {
			return err
		}
		return tx.UpsertWorker(w)
	})

	// An assignment that cannot fit must leave nothing behind.
	err := st.Update(ctx, func(tx *Tx) error {
		job, err := tx.GetJob("job_1")
		if err != nil {
			return err
		}
		worker, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		a := &types.Attempt{
			ID: "att_1", JobID: job.ID, Number: 1, WorkerID: worker.ID,
			State: state.AttemptAssigned, LeaseID: "lse_1",
			LeaseExpiresAt: time.Now().Add(time.Minute), Request: job.Request,
			CreatedAt: time.Now(),
		}
		if err := tx.InsertAttempt(a); err != nil {
			return err
		}
		job.AttemptCount++
		job.CurrentAttemptID = a.ID
		if err := tx.TransitionJob(job, state.JobAssigned, "assigned"); err != nil {
			return err
		}
		if err := tx.SaveJob(job); err != nil {
			return err
		}
		// Double-book on purpose: the second reservation exceeds capacity.
		worker.Allocated = worker.Allocated.Add(job.Request).Add(job.Request)
		return tx.SaveWorker(worker)
	})
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("err = %v, want ErrCapacityExceeded", err)
	}

	mustView(t, st, func(tx *Tx) error {
		job, err := tx.GetJob("job_1")
		if err != nil {
			return err
		}
		if job.State != state.JobQueued || job.AttemptCount != 0 || job.CurrentAttemptID != "" {
			t.Errorf("job survived the rollback as %+v; want an untouched QUEUED job", job)
		}
		attempts, err := tx.ListAttemptsForJob("job_1")
		if err != nil {
			return err
		}
		if len(attempts) != 0 {
			t.Errorf("%d attempt rows survived the rollback, want 0", len(attempts))
		}
		worker, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		if worker.Allocated != (types.Resources{}) {
			t.Errorf("worker allocation = %+v after rollback, want zero", worker.Allocated)
		}
		return nil
	})
}

// TestFinishAttemptReleasesCapacityExactlyOnce covers the double-release bug that
// resource accounting in a distributed scheduler is most prone to.
func TestFinishAttemptReleasesCapacityExactlyOnce(t *testing.T) {
	st := open(t)

	j := newJob("job_1", "key_1", 1000, 1<<30)
	w := newWorker("w1", 4000, 4<<30)
	var attempt *types.Attempt

	mustUpdate(t, st, func(tx *Tx) error {
		if err := tx.InsertJob(j); err != nil {
			return err
		}
		if err := tx.TransitionJob(j, state.JobQueued, "accepted"); err != nil {
			return err
		}
		if err := tx.UpsertWorker(w); err != nil {
			return err
		}
		attempt = &types.Attempt{
			ID: "att_1", JobID: j.ID, Number: 1, WorkerID: w.ID,
			State: state.AttemptAssigned, LeaseID: "lse_1",
			LeaseExpiresAt: time.Now().Add(time.Minute), Request: j.Request,
			CreatedAt: time.Now(),
		}
		if err := tx.InsertAttempt(attempt); err != nil {
			return err
		}
		worker, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		worker.Allocated = worker.Allocated.Add(j.Request)
		return tx.SaveWorker(worker)
	})

	mustUpdate(t, st, func(tx *Tx) error {
		a, err := tx.GetAttempt("att_1")
		if err != nil {
			return err
		}
		if err := tx.TransitionAttempt(a, state.AttemptRunning, "started"); err != nil {
			return err
		}
		return tx.FinishAttempt(a, state.AttemptSucceeded, "done")
	})

	mustView(t, st, func(tx *Tx) error {
		worker, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		if worker.Allocated != (types.Resources{}) {
			t.Errorf("allocation = %+v after one release, want zero", worker.Allocated)
		}
		return nil
	})

	// A second release attempt must be refused by the state machine, not applied.
	err := st.Update(context.Background(), func(tx *Tx) error {
		a, err := tx.GetAttempt("att_1")
		if err != nil {
			return err
		}
		return tx.FinishAttempt(a, state.AttemptLost, "double release")
	})
	var illegal *state.ErrIllegalTransition
	if !errors.As(err, &illegal) {
		t.Fatalf("second FinishAttempt err = %v, want an illegal-transition error", err)
	}
	mustView(t, st, func(tx *Tx) error {
		worker, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		if worker.Allocated.CPUMillis < 0 || worker.Allocated.MemoryBytes < 0 {
			t.Errorf("allocation went negative: %+v", worker.Allocated)
		}
		return nil
	})
}

func TestExpiredAttemptsFindsOnlyLapsedLiveLeases(t *testing.T) {
	st := open(t)
	now := time.Now().UTC()

	mustUpdate(t, st, func(tx *Tx) error {
		w := newWorker("w1", 8000, 8<<30)
		if err := tx.UpsertWorker(w); err != nil {
			return err
		}
		for i, spec := range []struct {
			id      string
			expires time.Time
			finish  bool
		}{
			{"lapsed", now.Add(-time.Minute), false},
			{"valid", now.Add(time.Minute), false},
			{"lapsed-but-done", now.Add(-time.Minute), true},
		} {
			j := newJob("job_"+spec.id, "key_"+spec.id, 100, 1<<20)
			if err := tx.InsertJob(j); err != nil {
				return err
			}
			if err := tx.TransitionJob(j, state.JobQueued, "accepted"); err != nil {
				return err
			}
			a := &types.Attempt{
				ID: "att_" + spec.id, JobID: j.ID, Number: int32(i + 1), WorkerID: "w1",
				State: state.AttemptAssigned, LeaseID: "lse_" + spec.id,
				LeaseExpiresAt: spec.expires, Request: j.Request, CreatedAt: now,
			}
			if err := tx.InsertAttempt(a); err != nil {
				return err
			}
			worker, err := tx.GetWorker("w1")
			if err != nil {
				return err
			}
			worker.Allocated = worker.Allocated.Add(a.Request)
			if err := tx.SaveWorker(worker); err != nil {
				return err
			}
			if spec.finish {
				if err := tx.TransitionAttempt(a, state.AttemptRunning, "started"); err != nil {
					return err
				}
				if err := tx.FinishAttempt(a, state.AttemptSucceeded, "done"); err != nil {
					return err
				}
			}
		}
		return nil
	})

	var expired []*types.Attempt
	mustView(t, st, func(tx *Tx) error {
		var err error
		expired, err = tx.ExpiredAttempts(now, 100)
		return err
	})
	if len(expired) != 1 || expired[0].ID != "att_lapsed" {
		ids := make([]string, len(expired))
		for i, a := range expired {
			ids[i] = a.ID
		}
		t.Fatalf("expired = %v, want exactly [att_lapsed]", ids)
	}
}

// TestTransitionAuditLog is what makes invariant I1 checkable over a whole run
// rather than only at the end: every state change is recorded in order.
func TestTransitionAuditLog(t *testing.T) {
	st := open(t)

	j := newJob("job_1", "key_1", 100, 1<<20)
	mustUpdate(t, st, func(tx *Tx) error {
		if err := tx.InsertJob(j); err != nil {
			return err
		}
		return tx.TransitionJob(j, state.JobQueued, "accepted")
	})
	mustUpdate(t, st, func(tx *Tx) error {
		loaded, err := tx.GetJob("job_1")
		if err != nil {
			return err
		}
		if err := tx.TransitionJob(loaded, state.JobAssigned, "assigned to w1"); err != nil {
			return err
		}
		return tx.TransitionJob(loaded, state.JobRunning, "started")
	})

	var log []types.Transition
	mustView(t, st, func(tx *Tx) error {
		var err error
		log, err = tx.Transitions("job_1")
		return err
	})

	want := []struct{ from, to string }{
		{"", "SUBMITTED"},
		{"SUBMITTED", "QUEUED"},
		{"QUEUED", "ASSIGNED"},
		{"ASSIGNED", "RUNNING"},
	}
	if len(log) != len(want) {
		t.Fatalf("audit log has %d entries, want %d: %+v", len(log), len(want), log)
	}
	for i, w := range want {
		if log[i].FromState != w.from || log[i].ToState != w.to {
			t.Errorf("entry %d = %s -> %s, want %s -> %s", i, log[i].FromState, log[i].ToState, w.from, w.to)
		}
		if i > 0 && log[i].Seq <= log[i-1].Seq {
			t.Errorf("audit sequence is not monotonic at entry %d", i)
		}
	}
}

func TestNotFoundIsDistinguishable(t *testing.T) {
	st := open(t)
	mustView(t, st, func(tx *Tx) error {
		if _, err := tx.GetJob("nope"); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetJob err = %v, want ErrNotFound", err)
		}
		if _, err := tx.GetAttempt("nope"); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetAttempt err = %v, want ErrNotFound", err)
		}
		if _, err := tx.GetWorker("nope"); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetWorker err = %v, want ErrNotFound", err)
		}
		return nil
	})
}

func TestWorkerGenerationIncrementsOnReregistration(t *testing.T) {
	st := open(t)
	w := newWorker("w1", 1000, 1<<30)
	mustUpdate(t, st, func(tx *Tx) error { return tx.UpsertWorker(w) })
	mustUpdate(t, st, func(tx *Tx) error { return tx.UpsertWorker(newWorker("w1", 2000, 2<<30)) })

	mustView(t, st, func(tx *Tx) error {
		got, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		if got.Generation != 2 {
			t.Errorf("generation = %d after re-registration, want 2", got.Generation)
		}
		if got.Capacity.CPUMillis != 2000 {
			t.Errorf("capacity was not refreshed: %+v", got.Capacity)
		}
		return nil
	})
}

func mustUpdate(t *testing.T, st *Store, fn func(*Tx) error) {
	t.Helper()
	if err := st.Update(context.Background(), fn); err != nil {
		t.Fatalf("update: %v", err)
	}
}

func mustView(t *testing.T, st *Store, fn func(*Tx) error) {
	t.Helper()
	if err := st.View(context.Background(), fn); err != nil {
		t.Fatalf("view: %v", err)
	}
}

//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/invariants"
	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
	"github.com/BruceMoseti/Atlas/internal/worker"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestWorkerDeathRecoversItsJobs is experiment 2 from the README, as a test.
//
// Kill a worker mid-flight and every job it was holding must end up finished
// somewhere else. "Eventually finished" is the property; which worker and how many
// attempts it took are not.
func TestWorkerDeathRecoversItsJobs(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	for _, id := range []string{"w1", "w2", "w3"} {
		c.startWorker(id, 2000, 2<<30)
	}

	// Each job occupies a full slot and runs long enough to still be in flight
	// when the kill lands.
	ids := c.submitN(30, []string{"sh", "-c", "sleep 0.4"}, 1000, 1<<30)

	waitUntilRunning(t, c, ids, 4, 15*time.Second)
	c.killWorker("w1")

	jobs := c.waitForTerminal(ids, 90*time.Second)

	succeeded := 0
	for id, j := range jobs {
		if j.State == pb.JobState_JOB_STATE_SUCCEEDED {
			succeeded++
			continue
		}
		t.Errorf("job %s ended as %s (%s); killing one of three workers should not fail any job",
			id, j.State, j.Message)
	}
	if succeeded != len(ids) {
		t.Errorf("%d of %d jobs succeeded", succeeded, len(ids))
	}

	rep := c.checkInvariants(ids)
	if rep.AttemptsLost == 0 {
		t.Error("no attempts were marked LOST; the kill did not exercise the recovery path")
	}
	t.Logf("recovered after a worker kill: %d attempts for %d jobs, %d lost, %d retries",
		rep.AttemptsTotal, len(ids), rep.AttemptsLost, rep.Retries)

	// Capacity held by the dead worker must come back, or the cluster shrinks
	// permanently every time a machine dies.
	c.requireAllocationDrains(10 * time.Second)
}

// TestKillingMostOfTheFleetStillCompletesEveryJob pushes the same property harder:
// a third of the fleet disappears while work is in flight.
func TestKillingMostOfTheFleetStillCompletesEveryJob(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	workers := []string{"w1", "w2", "w3", "w4", "w5", "w6"}
	for _, id := range workers {
		c.startWorker(id, 2000, 2<<30)
	}

	ids := c.submitN(60, []string{"sh", "-c", "sleep 0.3"}, 1000, 1<<30)
	waitUntilRunning(t, c, ids, 6, 15*time.Second)

	for _, id := range []string{"w1", "w2"} {
		c.killWorker(id)
		time.Sleep(300 * time.Millisecond)
	}

	jobs := c.waitForTerminal(ids, 120*time.Second)
	for id, j := range jobs {
		if j.State != pb.JobState_JOB_STATE_SUCCEEDED {
			t.Errorf("job %s ended as %s (%s)", id, j.State, j.Message)
		}
	}
	rep := c.checkInvariants(ids)
	t.Logf("survived losing 2 of 6 workers: %d attempts, %d lost, %d retries, max attempts on one job %d",
		rep.AttemptsTotal, rep.AttemptsLost, rep.Retries, rep.MaxAttemptsUsed)
}

// TestSchedulerRestartRecoversState is experiment 3: lose the control plane
// entirely, bring it back on the same database, and check that nothing was lost and
// nothing was resurrected.
func TestSchedulerRestartRecoversState(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	c := startCluster(t, cfg)

	c.startWorker("w1", 4000, 4<<30)
	c.startWorker("w2", 4000, 4<<30)

	// A mix of states at the moment of the crash: some already finished, some
	// running, some still queued behind them.
	finished := c.submitN(10, []string{"sh", "-c", "echo quick"}, 500, 512<<20)
	c.waitForTerminal(finished, 60*time.Second)

	inFlight := c.submitN(20, []string{"sh", "-c", "sleep 0.5"}, 1000, 1<<30)
	waitUntilRunning(t, c, inFlight, 4, 15*time.Second)

	// No graceful shutdown, no chance to flush anything that was not already
	// durable. The workers keep running throughout.
	c.restartScheduler(500 * time.Millisecond)

	// Jobs that had already finished must stay finished: a restart must not
	// resurrect terminal work (invariant I1).
	for _, id := range finished {
		j := c.getJob(id)
		if j.State != pb.JobState_JOB_STATE_SUCCEEDED {
			t.Errorf("job %s was SUCCEEDED before the crash and is %s after", id, j.State)
		}
	}

	// Jobs that were in flight must be reconciled and complete.
	jobs := c.waitForTerminal(inFlight, 120*time.Second)
	for id, j := range jobs {
		if j.State != pb.JobState_JOB_STATE_SUCCEEDED {
			t.Errorf("in-flight job %s ended as %s (%s)", id, j.State, j.Message)
		}
	}

	rep := c.checkInvariants(append(append([]string{}, finished...), inFlight...))
	t.Logf("after scheduler restart: %d jobs, %d terminal, %d attempts, %d lost, %d retries",
		rep.JobsTotal, rep.JobsTerminal, rep.AttemptsTotal, rep.AttemptsLost, rep.Retries)
	if rep.JobsTotal != len(finished)+len(inFlight) {
		t.Errorf("%d jobs in the store, want %d: no accepted job may disappear across a restart",
			rep.JobsTotal, len(finished)+len(inFlight))
	}
}

// TestSchedulerRestartWithNoWorkersRequeuesEverything isolates lease reclamation
// across a restart: the whole fleet is gone when the new scheduler starts, so every
// in-flight lease must lapse and every job must come back to the queue.
func TestSchedulerRestartWithNoWorkersRequeuesEverything(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	c := startCluster(t, cfg)

	c.startWorker("w1", 4000, 4<<30)
	ids := c.submitN(8, []string{"sh", "-c", "sleep 30"}, 1000, 1<<30)
	waitUntilRunning(t, c, ids, 4, 15*time.Second)

	// The worker dies too, so nothing can renew the leases taken before the
	// crash and recovery alone has to put the work back.
	c.killWorker("w1")
	c.restartScheduler(cfg.LeaseTTL + 500*time.Millisecond)

	deadline := time.Now().Add(20 * time.Second)
	var queued int
	for time.Now().Before(deadline) {
		st, err := c.client.GetClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
		if err == nil {
			queued = int(st.JobsQueued)
			if queued == len(ids) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if queued != len(ids) {
		t.Fatalf("%d jobs queued after recovery, want all %d", queued, len(ids))
	}

	// Allocation from the dead incarnation must not still be booked, or the
	// cluster permanently loses the capacity those jobs held.
	c.requireAllocationDrains(10 * time.Second)

	// Bring a worker back; the requeued work must run.
	c.startWorker("w2", 8000, 8<<30)
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		running := 0
		for _, id := range ids {
			if c.getJob(id).State == pb.JobState_JOB_STATE_RUNNING {
				running++
			}
		}
		if running == len(ids) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, id := range ids {
		if got := c.getJob(id).State; got == pb.JobState_JOB_STATE_QUEUED {
			t.Errorf("job %s never ran after the fleet came back", id)
		}
		if _, err := c.client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: id}); err != nil {
			t.Fatal(err)
		}
	}
	c.checkInvariants(ids)
}

// TestStaleAttemptCannotOverwriteCurrentResult is the scenario in
// docs/SEMANTICS.md §4, driven directly against the worker API.
//
// A worker is assigned a job, its lease is reclaimed, the job is reassigned, and
// then the original worker comes back and reports success. That report must not
// touch the job.
func TestStaleAttemptCannotOverwriteCurrentResult(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	c := startCluster(t, cfg)

	// Register a worker through the API but never run an agent for it, so
	// nothing renews its leases and nothing executes its assignments.
	workerClient := pb.NewWorkerServiceClient(c.conn)
	reg, err := workerClient.RegisterWorker(context.Background(), &pb.RegisterWorkerRequest{
		WorkerId: "ghost",
		Capacity: &pb.ResourceSpec{CpuMillis: 4000, MemoryBytes: 4 << 30},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "stale",
		Command:        []string{"sh", "-c", "echo hi"},
		CpuMillis:      1000,
		MemoryBytes:    1 << 30,
		MaxAttempts:    5,
	})

	// Collect the first assignment.
	acq, err := workerClient.AcquireJob(context.Background(), &pb.AcquireJobRequest{
		WorkerId: "ghost", Generation: reg.Generation, WaitMillis: 5000,
	})
	if err != nil || len(acq.Assignments) == 0 {
		t.Fatalf("acquire: %v (%d assignments)", err, len(acq.GetAssignments()))
	}
	first := acq.Assignments[0]
	firstRef := &pb.AttemptRef{
		JobId: first.JobId, AttemptId: first.AttemptId,
		LeaseId: first.LeaseId, WorkerId: "ghost",
	}
	if _, err := workerClient.StartJob(context.Background(), &pb.StartJobRequest{Ref: firstRef}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Stop renewing. The lease lapses, the attempt is declared LOST, and the
	// job comes back to the queue.
	waitFor(t, 20*time.Second, "attempt 1 to be reclaimed", func() bool {
		j := c.getJob(res.JobId)
		return len(j.Attempts) > 0 && j.Attempts[0].State == pb.AttemptState_ATTEMPT_STATE_LOST
	})

	// Now a real worker picks it up and finishes it.
	c.startWorker("real", 4000, 4<<30)
	job := c.waitForTerminal([]string{res.JobId}, 60*time.Second)[res.JobId]
	if job.State != pb.JobState_JOB_STATE_SUCCEEDED {
		t.Fatalf("job ended as %s, want SUCCEEDED", job.State)
	}
	winner := job.CurrentAttemptId
	if winner == first.AttemptId {
		t.Fatal("the job was decided by the attempt that was supposed to be reclaimed")
	}

	// The ghost comes back and reports success on its long-dead attempt.
	_, err = workerClient.CompleteJob(context.Background(), &pb.CompleteJobRequest{
		Ref: firstRef, ExitCode: 0, StdoutTail: "stale output",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale CompleteJob returned %v (code %s), want FAILED_PRECONDITION",
			err, status.Code(err))
	}

	after := c.getJob(res.JobId)
	if after.CurrentAttemptId != winner {
		t.Errorf("current attempt changed from %s to %s after a stale report", winner, after.CurrentAttemptId)
	}
	if after.State != pb.JobState_JOB_STATE_SUCCEEDED {
		t.Errorf("job state changed to %s after a stale report", after.State)
	}
	for _, a := range after.Attempts {
		if a.AttemptId == first.AttemptId && a.State != pb.AttemptState_ATTEMPT_STATE_LOST {
			t.Errorf("the reclaimed attempt is now %s; it must stay LOST", a.State)
		}
	}

	// Every other stale mutation must be refused too, not just completion.
	for name, call := range map[string]func() error{
		"StartJob": func() error {
			_, err := workerClient.StartJob(context.Background(), &pb.StartJobRequest{Ref: firstRef})
			return err
		},
		"RenewLease": func() error {
			_, err := workerClient.RenewLease(context.Background(), &pb.RenewLeaseRequest{Ref: firstRef})
			return err
		},
		"FailJob": func() error {
			_, err := workerClient.FailJob(context.Background(), &pb.FailJobRequest{
				Ref: firstRef, FailureClass: pb.FailureClass_FAILURE_CLASS_SYSTEM_ERROR,
			})
			return err
		},
	} {
		if code := status.Code(call()); code != codes.FailedPrecondition {
			t.Errorf("stale %s returned %s, want FAILED_PRECONDITION", name, code)
		}
	}

	c.checkInvariants([]string{res.JobId})
}

// TestForgedLeaseIsRejected checks the other half of attempt scoping: a caller that
// guesses a job and attempt id but not the lease id has no authority.
func TestForgedLeaseIsRejected(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	workerClient := pb.NewWorkerServiceClient(c.conn)

	reg, err := workerClient.RegisterWorker(context.Background(), &pb.RegisterWorkerRequest{
		WorkerId: "ghost",
		Capacity: &pb.ResourceSpec{CpuMillis: 4000, MemoryBytes: 4 << 30},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "forge",
		Command:        []string{"sh", "-c", "echo hi"},
		CpuMillis:      1000,
		MemoryBytes:    1 << 30,
	})
	acq, err := workerClient.AcquireJob(context.Background(), &pb.AcquireJobRequest{
		WorkerId: "ghost", Generation: reg.Generation, WaitMillis: 5000,
	})
	if err != nil || len(acq.Assignments) == 0 {
		t.Fatalf("acquire: %v", err)
	}
	a := acq.Assignments[0]

	_, err = workerClient.CompleteJob(context.Background(), &pb.CompleteJobRequest{
		Ref: &pb.AttemptRef{
			JobId: a.JobId, AttemptId: a.AttemptId,
			LeaseId: "lse_forged", WorkerId: "ghost",
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("forged lease returned %v (code %s), want FAILED_PRECONDITION", err, status.Code(err))
	}
	if j := c.getJob(res.JobId); j.State == pb.JobState_JOB_STATE_SUCCEEDED {
		t.Fatal("a forged lease completed the job")
	}
}

// TestDuplicateCompletionIsIdempotent covers the lost-response case: the worker's
// report landed but the acknowledgement did not, so the worker sends it again.
func TestDuplicateCompletionIsIdempotent(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	workerClient := pb.NewWorkerServiceClient(c.conn)

	reg, err := workerClient.RegisterWorker(context.Background(), &pb.RegisterWorkerRequest{
		WorkerId: "manual",
		Capacity: &pb.ResourceSpec{CpuMillis: 4000, MemoryBytes: 4 << 30},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "dup",
		Command:        []string{"sh", "-c", "echo hi"},
		CpuMillis:      1000,
		MemoryBytes:    1 << 30,
	})
	acq, err := workerClient.AcquireJob(context.Background(), &pb.AcquireJobRequest{
		WorkerId: "manual", Generation: reg.Generation, WaitMillis: 5000,
	})
	if err != nil || len(acq.Assignments) == 0 {
		t.Fatalf("acquire: %v", err)
	}
	a := acq.Assignments[0]
	ref := &pb.AttemptRef{JobId: a.JobId, AttemptId: a.AttemptId, LeaseId: a.LeaseId, WorkerId: "manual"}

	// Starting twice must be a no-op, not an error.
	if _, err := workerClient.StartJob(context.Background(), &pb.StartJobRequest{Ref: ref}); err != nil {
		t.Fatalf("first start: %v", err)
	}
	second, err := workerClient.StartJob(context.Background(), &pb.StartJobRequest{Ref: ref})
	if err != nil {
		t.Fatalf("duplicate start: %v", err)
	}
	if !second.AlreadyStarted {
		t.Error("duplicate StartJob did not report already_started")
	}

	first, err := workerClient.CompleteJob(context.Background(), &pb.CompleteJobRequest{Ref: ref, ExitCode: 0})
	if err != nil {
		t.Fatalf("first complete: %v", err)
	}
	if first.AlreadyRecorded {
		t.Error("the first completion reported already_recorded")
	}

	for i := 0; i < 3; i++ {
		again, err := workerClient.CompleteJob(context.Background(), &pb.CompleteJobRequest{Ref: ref, ExitCode: 0})
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if !again.AlreadyRecorded {
			t.Errorf("replay %d did not report already_recorded", i)
		}
		if again.JobState != pb.JobState_JOB_STATE_SUCCEEDED {
			t.Errorf("replay %d reported state %s, want SUCCEEDED", i, again.JobState)
		}
	}

	job := c.getJob(res.JobId)
	if job.AttemptCount != 1 {
		t.Errorf("attempt count = %d after four completion reports, want 1", job.AttemptCount)
	}

	// Four releases of the same capacity would drive the allocation negative,
	// which the store refuses; check it directly anyway.
	rep := c.checkInvariants([]string{res.JobId})
	if rep.AttemptsSucceeded != 1 {
		t.Errorf("%d successful attempts recorded, want 1", rep.AttemptsSucceeded)
	}
}

// TestDuplicateRPCsUnderLoad runs the whole fleet with workers that deliberately
// replay every report, which is what a flaky network looks like from the scheduler's
// side.
func TestDuplicateRPCsUnderLoad(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	for _, id := range []string{"w1", "w2", "w3"} {
		c.startWorker(id, 2000, 2<<30, func(cfg *worker.Config) {
			cfg.DuplicateRPCRate = 1.0
		})
	}

	ids := c.submitN(40, []string{"sh", "-c", "echo ok"}, 500, 512<<20)
	jobs := c.waitForTerminal(ids, 90*time.Second)
	for id, j := range jobs {
		if j.State != pb.JobState_JOB_STATE_SUCCEEDED {
			t.Errorf("job %s = %s (%s)", id, j.State, j.Message)
		}
		if j.AttemptCount != 1 {
			t.Errorf("job %s used %d attempts; duplicate reports must not cause retries", id, j.AttemptCount)
		}
	}
	c.checkInvariants(ids)
}

// TestWorkerRestartReclaimsItsOldWork: a worker that comes back with the same id is
// definitively not running what its previous incarnation held, so re-registration
// should reclaim those attempts immediately rather than waiting out their leases.
func TestWorkerRestartReclaimsItsOldWork(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	cfg.LeaseTTL = 60 * time.Second // long, so only re-registration can reclaim
	c := startCluster(t, cfg)

	c.startWorker("w1", 2000, 2<<30)
	ids := c.submitN(2, []string{"sh", "-c", "sleep 30"}, 1000, 1<<30)
	waitUntilRunning(t, c, ids, 2, 15*time.Second)

	c.killWorker("w1")
	c.startWorker("w1", 2000, 2<<30)

	// Re-registration should have requeued the old work and the fresh worker
	// should pick it up, long before the 60s leases would have expired.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			if c.getJob(id).AttemptCount < 2 {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, id := range ids {
		j := c.getJob(id)
		if j.AttemptCount < 2 {
			t.Errorf("job %s is still on attempt %d; re-registration did not reclaim the old attempt",
				id, j.AttemptCount)
		}
		if len(j.Attempts) > 0 && j.Attempts[0].State != pb.AttemptState_ATTEMPT_STATE_LOST {
			t.Errorf("job %s attempt 1 is %s, want LOST", id, j.Attempts[0].State)
		}
	}

	for _, id := range ids {
		if _, err := c.client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: id}); err != nil {
			t.Fatal(err)
		}
	}
	c.checkInvariants(ids)
}

// TestSuspectWorkerKeepsItsWork: a single missed heartbeat must not cost a worker
// its assignments, or every GC pause becomes a round of duplicate executions.
func TestSuspectWorkerKeepsItsWork(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	cfg.LeaseTTL = 30 * time.Second
	cfg.SuspectAfter = 400 * time.Millisecond
	cfg.DeadAfter = 30 * time.Second // never reached during this test
	c := startCluster(t, cfg)

	// This worker heartbeats rarely enough to be SUSPECT almost immediately,
	// but keeps renewing its leases.
	c.startWorker("flaky", 2000, 2<<30, func(wc *worker.Config) {
		wc.HeartbeatDropRate = 1.0
	})
	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "suspect",
		Command:        []string{"sh", "-c", "sleep 2; echo done"},
		CpuMillis:      1000,
		MemoryBytes:    1 << 30,
	})

	waitFor(t, 15*time.Second, "the worker to be marked SUSPECT", func() bool {
		ws, err := c.client.ListWorkers(context.Background(), &pb.ListWorkersRequest{})
		if err != nil {
			return false
		}
		for _, w := range ws.Workers {
			if w.WorkerId == "flaky" && w.State == pb.WorkerState_WORKER_STATE_SUSPECT {
				return true
			}
		}
		return false
	})

	job := c.waitForTerminal([]string{res.JobId}, 60*time.Second)[res.JobId]
	if job.State != pb.JobState_JOB_STATE_SUCCEEDED {
		t.Fatalf("job ended as %s; a SUSPECT worker should keep and finish its work", job.State)
	}
	if job.AttemptCount != 1 {
		t.Errorf("attempt count = %d; being SUSPECT must not cost a worker its assignment", job.AttemptCount)
	}
	c.checkInvariants([]string{res.JobId})
}

// TestLeaseExpiryReclaimsAStuckWorker covers the path heartbeats cannot see: the
// worker process is alive and heartbeating, but has stopped renewing one lease.
func TestLeaseExpiryReclaimsAStuckWorker(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	cfg.LeaseTTL = 1500 * time.Millisecond
	cfg.DeadAfter = 60 * time.Second // heartbeats keep flowing, so this never fires
	c := startCluster(t, cfg)

	c.startWorker("stuck", 2000, 2<<30, func(wc *worker.Config) {
		wc.RenewDropRate = 1.0
	})
	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "lease-expiry",
		Command:        []string{"sh", "-c", "sleep 30"},
		CpuMillis:      1000,
		MemoryBytes:    1 << 30,
		MaxAttempts:    2,
	})

	waitFor(t, 20*time.Second, "the lease to be reclaimed", func() bool {
		j := c.getJob(res.JobId)
		return len(j.Attempts) > 0 && j.Attempts[0].State == pb.AttemptState_ATTEMPT_STATE_LOST
	})

	// The worker is still healthy throughout: only the lease was lost.
	ws, err := c.client.ListWorkers(context.Background(), &pb.ListWorkersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range ws.Workers {
		if w.WorkerId == "stuck" && w.State == pb.WorkerState_WORKER_STATE_DEAD {
			t.Error("the worker was declared DEAD; this test is meant to exercise lease expiry alone")
		}
	}

	if _, err := c.client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: res.JobId}); err != nil {
		t.Fatal(err)
	}
	c.checkInvariants([]string{res.JobId})
}

// TestRecoveryRebuildsAllocationFromLiveAttempts covers invariant I3 across a
// restart, with the denormalized columns deliberately corrupted first.
func TestRecoveryRebuildsAllocationFromLiveAttempts(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	cfg.LeaseTTL = 60 * time.Second
	c := startCluster(t, cfg)

	c.startWorker("w1", 4000, 4<<30)
	ids := c.submitN(2, []string{"sh", "-c", "sleep 30"}, 1000, 1<<30)
	waitUntilRunning(t, c, ids, 2, 15*time.Second)

	// Corrupt the denormalized allocation while the scheduler is down, which is
	// exactly the state a bug in the scheduler would leave behind.
	c.restartScheduler(100*time.Millisecond, func(dbPath string) {
		st, err := store.Open(store.Options{Path: dbPath})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if err := st.Update(context.Background(), func(tx *store.Tx) error {
			w, err := tx.GetWorker("w1")
			if err != nil {
				return err
			}
			w.Allocated = types.Resources{CPUMillis: 3500, MemoryBytes: 3 << 30}
			return tx.SaveWorker(w)
		}); err != nil {
			t.Fatal(err)
		}
	})

	rep, err := invariants.Check(context.Background(), c.store)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		t.Fatalf("recovery did not repair the corrupted allocation:\n%s", rep)
	}

	var allocated types.Resources
	if err := c.store.View(context.Background(), func(tx *store.Tx) error {
		w, err := tx.GetWorker("w1")
		if err != nil {
			return err
		}
		allocated = w.Allocated
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := types.Resources{CPUMillis: 2000, MemoryBytes: 2 << 30}
	if allocated != want {
		t.Errorf("allocation after recovery = %+v, want %+v recomputed from the two live attempts",
			allocated, want)
	}

	for _, id := range ids {
		if _, err := c.client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: id}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestHeartbeatTellsWorkersToAbandonReclaimedWork: the scheduler does not wait for a
// stale worker to report; it tells it to stop as soon as it notices.
func TestHeartbeatTellsWorkersToAbandonReclaimedWork(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	cfg.LeaseTTL = 1 * time.Second
	c := startCluster(t, cfg)

	workerClient := pb.NewWorkerServiceClient(c.conn)
	reg, err := workerClient.RegisterWorker(context.Background(), &pb.RegisterWorkerRequest{
		WorkerId: "ghost",
		Capacity: &pb.ResourceSpec{CpuMillis: 4000, MemoryBytes: 4 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "abandon",
		Command:        []string{"sh", "-c", "sleep 30"},
		CpuMillis:      1000,
		MemoryBytes:    1 << 30,
		MaxAttempts:    5,
	})
	acq, err := workerClient.AcquireJob(context.Background(), &pb.AcquireJobRequest{
		WorkerId: "ghost", Generation: reg.Generation, WaitMillis: 5000,
	})
	if err != nil || len(acq.Assignments) == 0 {
		t.Fatalf("acquire: %v", err)
	}
	attemptID := acq.Assignments[0].AttemptId

	// Keep heartbeating (so the worker stays alive) but never renew the lease.
	waitFor(t, 20*time.Second, "the scheduler to order the attempt abandoned", func() bool {
		hb, err := workerClient.Heartbeat(context.Background(), &pb.HeartbeatRequest{
			WorkerId: "ghost", Generation: reg.Generation,
			ActiveAttemptIds: []string{attemptID},
		})
		if err != nil {
			return false
		}
		for _, id := range hb.CancelAttemptIds {
			if id == attemptID {
				return true
			}
		}
		return false
	})

	if _, err := c.client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: res.JobId}); err != nil {
		t.Fatal(err)
	}
	c.checkInvariants([]string{res.JobId})
}

// --- helpers -----------------------------------------------------------------

// waitUntilRunning blocks until at least n of the given jobs have been handed to a
// worker. It reads each job's own state rather than a cluster gauge, so a test that
// says "two jobs are in flight" is asserting on durable facts.
func waitUntilRunning(t *testing.T, c *cluster, ids []string, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		live := 0
		for _, id := range ids {
			switch c.getJob(id).State {
			case pb.JobState_JOB_STATE_ASSIGNED, pb.JobState_JOB_STATE_RUNNING:
				live++
			}
		}
		if live >= n {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("fewer than %d of %d jobs reached a worker within %s", n, len(ids), timeout)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

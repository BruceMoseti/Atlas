//go:build integration

package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestJobRunsEndToEnd(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 4000, 4<<30)

	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "hello",
		Command:        []string{"sh", "-c", "echo hello atlas"},
	})
	jobs := c.waitForTerminal([]string{res.JobId}, 30*time.Second)
	job := jobs[res.JobId]

	if job.State != pb.JobState_JOB_STATE_SUCCEEDED {
		t.Fatalf("state = %s, want SUCCEEDED (message: %s)", job.State, job.Message)
	}
	if !job.HasExitCode || job.ExitCode != 0 {
		t.Errorf("exit code = %d (has=%v), want 0", job.ExitCode, job.HasExitCode)
	}
	if job.AttemptCount != 1 {
		t.Errorf("attempt count = %d, want 1: a clean run should not retry", job.AttemptCount)
	}
	if len(job.Attempts) != 1 {
		t.Fatalf("%d attempt records, want 1", len(job.Attempts))
	}
	if got := job.Attempts[0].StdoutTail; !strings.Contains(got, "hello atlas") {
		t.Errorf("stdout tail = %q, want it to contain the workload's output", got)
	}
	c.checkInvariants([]string{res.JobId})
}

func TestFailingJobIsNotRetriedByDefault(t *testing.T) {
	// A deterministic program that exits 7 will exit 7 again. Retrying it burns
	// capacity to produce the same answer, so PROCESS_EXIT is not retryable
	// unless the submitter opts in.
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 4000, 4<<30)

	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "fails",
		Command:        []string{"sh", "-c", "exit 7"},
		MaxAttempts:    5,
	})
	job := c.waitForTerminal([]string{res.JobId}, 30*time.Second)[res.JobId]

	if job.State != pb.JobState_JOB_STATE_FAILED {
		t.Fatalf("state = %s, want FAILED", job.State)
	}
	if job.AttemptCount != 1 {
		t.Errorf("attempt count = %d, want 1: process exits are not retried by default", job.AttemptCount)
	}
	if job.FailureClass != pb.FailureClass_FAILURE_CLASS_PROCESS_EXIT {
		t.Errorf("failure class = %s, want PROCESS_EXIT", job.FailureClass)
	}
	if !job.HasExitCode || job.ExitCode != 7 {
		t.Errorf("exit code = %d, want 7", job.ExitCode)
	}
	c.checkInvariants([]string{res.JobId})
}

func TestRetryOnProcessExitExhaustsTheBudget(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 4000, 4<<30)

	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey:     "retries",
		Command:            []string{"sh", "-c", "exit 1"},
		MaxAttempts:        3,
		RetryOnProcessExit: true,
	})
	job := c.waitForTerminal([]string{res.JobId}, 30*time.Second)[res.JobId]

	if job.State != pb.JobState_JOB_STATE_FAILED {
		t.Fatalf("state = %s, want FAILED", job.State)
	}
	// Invariant I5: exactly the budget, no more and no fewer.
	if job.AttemptCount != 3 {
		t.Errorf("attempt count = %d, want exactly the budget of 3", job.AttemptCount)
	}
	if len(job.Attempts) != 3 {
		t.Errorf("%d attempt records, want 3", len(job.Attempts))
	}
	c.checkInvariants([]string{res.JobId})
}

func TestTimeoutKillsTheWorkload(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 4000, 4<<30)

	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "timeout",
		Command:        []string{"sleep", "60"},
		TimeoutSeconds: 1,
		MaxAttempts:    2,
	})
	job := c.waitForTerminal([]string{res.JobId}, 30*time.Second)[res.JobId]

	if job.State != pb.JobState_JOB_STATE_FAILED {
		t.Fatalf("state = %s, want FAILED", job.State)
	}
	if job.FailureClass != pb.FailureClass_FAILURE_CLASS_TIMEOUT {
		t.Errorf("failure class = %s, want TIMEOUT", job.FailureClass)
	}
	if job.AttemptCount != 1 {
		t.Errorf("attempt count = %d, want 1: timeouts are not retried by default", job.AttemptCount)
	}
	c.checkInvariants([]string{res.JobId})
}

// TestIdempotentSubmission is the scenario a client cannot distinguish from the
// outside: the request succeeded but the response was lost.
func TestIdempotentSubmission(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 4000, 4<<30)

	req := &pb.SubmitJobRequest{
		IdempotencyKey: "same-key",
		Command:        []string{"sh", "-c", "echo once"},
		CpuMillis:      100,
		MemoryBytes:    16 << 20,
		MaxAttempts:    3,
	}
	first, err := c.client.SubmitJob(context.Background(), req)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if first.Deduplicated {
		t.Error("the first submission should not report deduplication")
	}

	for i := 0; i < 5; i++ {
		again, err := c.client.SubmitJob(context.Background(), req)
		if err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
		if again.JobId != first.JobId {
			t.Fatalf("retry %d created job %s, want the original %s", i, again.JobId, first.JobId)
		}
		if !again.Deduplicated {
			t.Errorf("retry %d did not report deduplication", i)
		}
	}

	c.waitForTerminal([]string{first.JobId}, 30*time.Second)
	jobs, err := c.client.ListJobs(context.Background(), &pb.ListJobsRequest{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Jobs) != 1 {
		t.Fatalf("%d jobs exist after six submissions of one key, want 1", len(jobs.Jobs))
	}
	c.checkInvariants([]string{first.JobId})
}

// TestIdempotencyKeyReuseWithDifferentSpecIsRejected: silently returning the old job
// would leave the client believing it submitted work Atlas never saw.
func TestIdempotencyKeyReuseWithDifferentSpecIsRejected(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 4000, 4<<30)

	base := &pb.SubmitJobRequest{
		IdempotencyKey: "reused",
		Command:        []string{"sh", "-c", "echo a"},
		CpuMillis:      100,
		MemoryBytes:    16 << 20,
		MaxAttempts:    3,
	}
	if _, err := c.client.SubmitJob(context.Background(), base); err != nil {
		t.Fatalf("first submit: %v", err)
	}

	different := &pb.SubmitJobRequest{
		IdempotencyKey: "reused",
		Command:        []string{"sh", "-c", "echo b"},
		CpuMillis:      100,
		MemoryBytes:    16 << 20,
		MaxAttempts:    3,
	}
	_, err := c.client.SubmitJob(context.Background(), different)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("err = %v (code %s), want ALREADY_EXISTS", err, status.Code(err))
	}
}

func TestCancelStopsARunningJob(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 4000, 4<<30)

	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "cancel-me",
		Command:        []string{"sleep", "120"},
		MaxAttempts:    3,
	})

	// Wait until it is actually executing, so the test covers cancellation of
	// running work rather than of a queued job.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if c.getJob(res.JobId).State == pb.JobState_JOB_STATE_RUNNING {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancelRes, err := c.client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: res.JobId, Reason: "test"})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !cancelRes.Canceled {
		t.Fatalf("cancel reported canceled=false for a running job (state %s)", cancelRes.State)
	}

	job := c.getJob(res.JobId)
	if job.State != pb.JobState_JOB_STATE_CANCELED {
		t.Fatalf("state = %s, want CANCELED", job.State)
	}
	// A canceled job must not be retried, however much budget remains.
	time.Sleep(2 * time.Second)
	if got := c.getJob(res.JobId); got.State != pb.JobState_JOB_STATE_CANCELED {
		t.Fatalf("state drifted to %s after cancellation", got.State)
	}

	// Capacity must come back, or a cancel would leak the machine.
	st, err := c.client.GetClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Allocated.CpuMillis != 0 {
		t.Errorf("allocated cpu = %d after cancellation, want 0", st.Allocated.CpuMillis)
	}
	c.checkInvariants([]string{res.JobId})
}

func TestCancelIsIdempotent(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 4000, 4<<30)

	res := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "cancel-twice",
		Command:        []string{"sh", "-c", "echo hi"},
	})
	c.waitForTerminal([]string{res.JobId}, 30*time.Second)

	// Cancelling a finished job is not an error: a client retrying a cancel it
	// is unsure about should not be punished for succeeding twice.
	again, err := c.client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: res.JobId})
	if err != nil {
		t.Fatalf("cancel of a terminal job: %v", err)
	}
	if again.Canceled {
		t.Error("cancel reported canceled=true for an already-terminal job")
	}
	if again.State != pb.JobState_JOB_STATE_SUCCEEDED {
		t.Errorf("state = %s, want the original SUCCEEDED to be preserved", again.State)
	}
}

func TestResourceAccountingIsReleasedOnCompletion(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 2000, 2<<30)

	// Each job takes half the worker, so at most two run at once and the third
	// has to wait for capacity to come back.
	ids := c.submitN(6, []string{"sh", "-c", "sleep 0.2"}, 1000, 1<<30)
	c.waitForTerminal(ids, 60*time.Second)

	st, err := c.client.GetClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Allocated.CpuMillis != 0 || st.Allocated.MemoryBytes != 0 {
		t.Errorf("allocation = cpu %d mem %d after everything finished, want zero",
			st.Allocated.CpuMillis, st.Allocated.MemoryBytes)
	}
	c.checkInvariants(ids)
}

func TestAdmissionControlRejectsUnschedulableJobs(t *testing.T) {
	// A job larger than any machine would otherwise sit in the queue forever,
	// occupying a slot and misrepresenting demand.
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 2000, 2<<30)

	_, err := c.client.SubmitJob(context.Background(), &pb.SubmitJobRequest{
		IdempotencyKey: "too-big",
		Command:        []string{"true"},
		CpuMillis:      64000,
		MemoryBytes:    1 << 30,
		MaxAttempts:    1,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v (code %s), want FAILED_PRECONDITION", err, status.Code(err))
	}
}

func TestAdmissionControlBoundsTheQueue(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	cfg.Admission.MaxQueueDepth = 5
	c := startCluster(t, cfg)
	// No workers: nothing drains, so the queue fills and stays full.

	accepted, rejected := 0, 0
	for i := 0; i < 50; i++ {
		_, err := c.client.SubmitJob(context.Background(), &pb.SubmitJobRequest{
			IdempotencyKey: "fill-" + time.Now().Format(time.RFC3339Nano) + "-" + string(rune('a'+i%26)),
			Command:        []string{"true"},
			CpuMillis:      100,
			MemoryBytes:    16 << 20,
			MaxAttempts:    1,
		})
		switch {
		case err == nil:
			accepted++
		case status.Code(err) == codes.ResourceExhausted:
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if accepted != 5 {
		t.Errorf("accepted %d jobs, want exactly the queue limit of 5", accepted)
	}
	if rejected != 45 {
		t.Errorf("rejected %d jobs, want 45", rejected)
	}
}

func TestDrainStopsNewPlacementsButLetsRunningWorkFinish(t *testing.T) {
	c := startCluster(t, fastConfig(scheduler.BestFit{}))
	c.startWorker("w1", 1000, 1<<30)
	c.startWorker("w2", 1000, 1<<30)

	// Occupy w1 completely, then drain it.
	longRunning := c.submit(&pb.SubmitJobRequest{
		IdempotencyKey: "long",
		Command:        []string{"sh", "-c", "sleep 2"},
		CpuMillis:      1000,
		MemoryBytes:    1 << 30,
	})
	deadline := time.Now().Add(10 * time.Second)
	var busyWorker string
	for time.Now().Before(deadline) && busyWorker == "" {
		j := c.getJob(longRunning.JobId)
		if len(j.Attempts) > 0 && j.State == pb.JobState_JOB_STATE_RUNNING {
			busyWorker = j.Attempts[0].WorkerId
		}
		time.Sleep(20 * time.Millisecond)
	}
	if busyWorker == "" {
		t.Fatal("the long-running job never started")
	}

	if _, err := c.client.DrainWorker(context.Background(), &pb.DrainWorkerRequest{WorkerId: busyWorker}); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// Everything submitted now must land on the other worker.
	ids := c.submitN(4, []string{"sh", "-c", "echo ok"}, 1000, 1<<30)
	jobs := c.waitForTerminal(ids, 60*time.Second)
	for id, j := range jobs {
		if j.State != pb.JobState_JOB_STATE_SUCCEEDED {
			t.Fatalf("job %s = %s, want SUCCEEDED", id, j.State)
		}
		for _, a := range j.Attempts {
			if a.WorkerId == busyWorker {
				t.Errorf("job %s was placed on drained worker %s", id, busyWorker)
			}
		}
	}

	// The job that was already running when the drain started must still finish.
	finished := c.waitForTerminal([]string{longRunning.JobId}, 30*time.Second)[longRunning.JobId]
	if finished.State != pb.JobState_JOB_STATE_SUCCEEDED {
		t.Errorf("the draining worker's in-flight job ended as %s, want SUCCEEDED", finished.State)
	}
	c.checkInvariants(append(ids, longRunning.JobId))
}

func TestPriorityOrdersExecution(t *testing.T) {
	cfg := fastConfig(scheduler.BestFit{})
	cfg.Queue.Ordering = scheduler.OrderPriority
	c := startCluster(t, cfg)

	// Submit before any worker exists, so everything queues up and the ordering
	// is what decides execution order rather than arrival timing.
	var ids []string
	for i := 0; i < 8; i++ {
		prio := int32(0)
		if i >= 4 {
			prio = 100
		}
		res := c.submit(&pb.SubmitJobRequest{
			IdempotencyKey: "prio-" + string(rune('a'+i)),
			Command:        []string{"sh", "-c", "echo ok"},
			CpuMillis:      1000,
			MemoryBytes:    1 << 30,
			Priority:       prio,
		})
		ids = append(ids, res.JobId)
	}

	// One slot at a time, so execution order is fully determined by the queue.
	c.startWorker("w1", 1000, 1<<30)
	jobs := c.waitForTerminal(ids, 90*time.Second)

	type started struct {
		prio int32
		at   int64
	}
	var order []started
	for _, j := range jobs {
		if len(j.Attempts) == 0 {
			t.Fatalf("job %s has no attempts", j.JobId)
		}
		order = append(order, started{prio: j.Priority, at: j.Attempts[0].CreatedAtUnixNanos})
	}
	// Every high-priority job must have been assigned before every
	// low-priority one.
	var lastHigh, firstLow int64
	firstLow = 1<<63 - 1
	for _, o := range order {
		if o.prio == 100 && o.at > lastHigh {
			lastHigh = o.at
		}
		if o.prio == 0 && o.at < firstLow {
			firstLow = o.at
		}
	}
	if lastHigh > firstLow {
		t.Errorf("a low-priority job was assigned before the last high-priority one under strict priority")
	}
	c.checkInvariants(ids)
}

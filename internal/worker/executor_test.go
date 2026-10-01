package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/types"
)

func assignment(command ...string) scheduler.Assignment {
	return scheduler.Assignment{
		JobID:         "job_test",
		AttemptID:     "att_test",
		AttemptNumber: 2,
		LeaseID:       "lse_test",
		Command:       command,
		Request:       types.Resources{CPUMillis: 1000, MemoryBytes: 256 << 20},
	}
}

func TestProcessExecutorCapturesOutputAndExitCode(t *testing.T) {
	a := assignment("sh", "-c", "echo to-stdout; echo to-stderr >&2; exit 0")
	res, err := ProcessExecutor{}.Run(context.Background(), a)
	if err != nil {
		t.Fatalf("Run returned an executor error: %v", err)
	}
	if !res.Succeeded() {
		t.Fatalf("expected success, got %+v", res)
	}
	if !strings.Contains(res.StdoutTail, "to-stdout") {
		t.Errorf("stdout tail = %q", res.StdoutTail)
	}
	if !strings.Contains(res.StderrTail, "to-stderr") {
		t.Errorf("stderr tail = %q", res.StderrTail)
	}
}

// A workload exiting non-zero is a result, not an executor error. Conflating the
// two is how a scheduler ends up retrying deterministic failures forever.
func TestProcessExecutorReportsNonZeroExitAsAResult(t *testing.T) {
	res, err := ProcessExecutor{}.Run(context.Background(), assignment("sh", "-c", "exit 7"))
	if err != nil {
		t.Fatalf("a failing workload must not produce an executor error, got %v", err)
	}
	if res.ExitCode != 7 {
		t.Errorf("exit code = %d, want 7", res.ExitCode)
	}
	if res.FailureClass != state.FailureProcessExit {
		t.Errorf("failure class = %s, want PROCESS_EXIT", res.FailureClass)
	}
	if res.Succeeded() {
		t.Error("Succeeded() returned true for exit code 7")
	}
}

func TestProcessExecutorEnforcesTheTimeout(t *testing.T) {
	a := assignment("sleep", "30")
	a.Timeout = 300 * time.Millisecond

	start := time.Now()
	res, err := ProcessExecutor{}.Run(context.Background(), a)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run returned an executor error: %v", err)
	}
	if res.FailureClass != state.FailureTimeout {
		t.Errorf("failure class = %s, want TIMEOUT", res.FailureClass)
	}
	if elapsed > 5*time.Second {
		t.Errorf("the workload ran for %s despite a 300ms timeout", elapsed)
	}
}

// TestProcessExecutorKillsTheWholeProcessGroup is the reason for Setpgid. A
// workload that spawns children and is killed must not leak them: the scheduler
// has already decided that capacity is free.
func TestProcessExecutorKillsTheWholeProcessGroup(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "child-survived")

	// The child outlives its parent shell and writes the marker 2s later. If
	// only the shell is killed, the marker appears.
	a := assignment("sh", "-c", "(sleep 2; touch "+marker+") & sleep 30")
	a.Timeout = 200 * time.Millisecond

	if _, err := (ProcessExecutor{}).Run(context.Background(), a); err != nil {
		t.Fatalf("Run: %v", err)
	}
	time.Sleep(3 * time.Second)

	if _, err := os.Stat(marker); err == nil {
		t.Fatal("a child process outlived the killed workload; the process group was not killed")
	}
}

func TestProcessExecutorReportsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()

	res, err := ProcessExecutor{}.Run(ctx, assignment("sleep", "30"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.FailureClass != state.FailureCanceled {
		t.Errorf("failure class = %s, want CANCELED", res.FailureClass)
	}
}

// A binary that does not exist is the worker's problem, not the workload's, so it
// is SYSTEM_ERROR (retryable elsewhere) rather than PROCESS_EXIT.
func TestProcessExecutorClassifiesStartFailureAsSystemError(t *testing.T) {
	res, err := ProcessExecutor{}.Run(context.Background(), assignment("definitely-not-a-real-binary-xyz"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.FailureClass != state.FailureSystemError {
		t.Errorf("failure class = %s, want SYSTEM_ERROR", res.FailureClass)
	}
}

func TestProcessExecutorRejectsAnEmptyCommand(t *testing.T) {
	res, err := ProcessExecutor{}.Run(context.Background(), assignment())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.FailureClass != state.FailureSystemError {
		t.Errorf("failure class = %s, want SYSTEM_ERROR", res.FailureClass)
	}
}

// TestExecutionIdentifiersReachTheWorkload covers the contract in
// docs/SEMANTICS.md §1: a workload with external side effects needs a stable
// identifier to make them idempotent, and ATLAS_JOB_ID is it.
func TestExecutionIdentifiersReachTheWorkload(t *testing.T) {
	a := assignment("sh", "-c", "echo $ATLAS_JOB_ID $ATLAS_ATTEMPT_ID $ATLAS_ATTEMPT_NUMBER $ATLAS_CPU_MILLIS $CUSTOM")
	a.Env = map[string]string{"CUSTOM": "value"}

	res, err := ProcessExecutor{}.Run(context.Background(), a)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := strings.TrimSpace(res.StdoutTail)
	for _, want := range []string{"job_test", "att_test", "2", "1000", "value"} {
		if !strings.Contains(out, want) {
			t.Errorf("workload environment is missing %q; got %q", want, out)
		}
	}
}

// TestTailBufferKeepsOnlyTheTail: a chatty workload must not be able to exhaust
// the worker's memory.
func TestTailBufferKeepsOnlyTheTail(t *testing.T) {
	var b tailBuffer
	chunk := strings.Repeat("a", 1000)
	for i := 0; i < 20; i++ {
		n, err := b.Write([]byte(chunk))
		if err != nil || n != len(chunk) {
			t.Fatalf("Write returned (%d, %v); it must report the full length it was given", n, err)
		}
	}
	if got := len(b.String()); got != tailBytes {
		t.Fatalf("buffer holds %d bytes after 20KiB of writes, want exactly %d", got, tailBytes)
	}

	// A single write larger than the buffer must also be truncated to the tail.
	var b2 tailBuffer
	b2.Write([]byte(strings.Repeat("x", tailBytes*3) + "END"))
	s := b2.String()
	if len(s) != tailBytes {
		t.Fatalf("after one oversized write the buffer holds %d bytes, want %d", len(s), tailBytes)
	}
	if !strings.HasSuffix(s, "END") {
		t.Error("the buffer kept the head of an oversized write rather than its tail")
	}
}

func TestLargeOutputIsTruncatedNotStreamed(t *testing.T) {
	// 200k lines of output must not grow the reported tail beyond the cap.
	a := assignment("sh", "-c", "i=0; while [ $i -lt 20000 ]; do echo 0123456789; i=$((i+1)); done")
	res, err := ProcessExecutor{}.Run(context.Background(), a)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.StdoutTail) > tailBytes {
		t.Fatalf("stdout tail is %d bytes, want at most %d", len(res.StdoutTail), tailBytes)
	}
}

func TestDockerExecutorRequiresAnImage(t *testing.T) {
	// The Docker executor cannot be exercised here (no daemon in CI), but the
	// argument validation it does before touching Docker can be.
	res, err := DockerExecutor{}.Run(context.Background(), assignment("echo", "hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.FailureClass != state.FailureSystemError {
		t.Errorf("failure class = %s, want SYSTEM_ERROR for a missing image", res.FailureClass)
	}
}

func TestDockerExecutorDefaults(t *testing.T) {
	d := DockerExecutor{}
	if d.binary() != "docker" {
		t.Errorf("binary() = %q, want docker", d.binary())
	}
	// A batch workload that needs the network should have to say so.
	if d.network() != "none" {
		t.Errorf("network() = %q, want none", d.network())
	}
	if got := (DockerExecutor{Network: "bridge"}).network(); got != "bridge" {
		t.Errorf("network() = %q, want bridge", got)
	}
}

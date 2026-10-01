// Package worker is the Atlas execution plane: the agent that registers with the
// scheduler, pulls assignments, runs them, renews their leases, and reports results.
package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/state"
)

// tailBytes is how much of each stream is kept and reported. Atlas is a scheduler,
// not a log store; the tail is for diagnosing a failure at a glance, and anything
// more belongs in the workload's own logging.
const tailBytes = 4096

// Result is the outcome of one execution.
type Result struct {
	ExitCode     int32
	FailureClass state.FailureClass
	Message      string
	StdoutTail   string
	StderrTail   string
}

// Succeeded reports whether the workload exited cleanly.
func (r Result) Succeeded() bool {
	return r.FailureClass == state.FailureNone && r.ExitCode == 0
}

// Executor runs one assignment to completion.
//
// Two implementations exist because they are genuinely different execution
// environments, not because the indirection might be useful later: the process
// executor runs the command directly, and the Docker executor runs it in a container
// with enforced resource limits.
type Executor interface {
	// Name identifies the executor in logs and in the worker's advertised
	// labels.
	Name() string
	// Run executes the assignment. Cancelling ctx must terminate the workload.
	// Run returns an error only for executor faults; a workload that fails is a
	// Result with a non-zero exit code, not an error.
	Run(ctx context.Context, a scheduler.Assignment) (Result, error)
}

// ProcessExecutor runs the assignment's command as a child process.
//
// Resource requests are advertised to the workload through the environment but are
// not enforced: a process executor has no way to cap CPU or memory without cgroups.
// The scheduler's accounting still holds, because it only ever places work whose
// declared request fits. Use the Docker executor when you need enforcement.
type ProcessExecutor struct{}

func (ProcessExecutor) Name() string { return "process" }

func (ProcessExecutor) Run(ctx context.Context, a scheduler.Assignment) (Result, error) {
	if len(a.Command) == 0 {
		return Result{
			FailureClass: state.FailureSystemError,
			Message:      "assignment has no command",
		}, nil
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if a.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, a.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(runCtx, a.Command[0], a.Command[1:]...)
	cmd.Env = append(baseEnv(a), flattenEnv(a.Env)...)

	// A new process group means killing the job kills everything it started.
	// Without this, a workload that spawns children leaks them when its lease is
	// reclaimed, and the "released" capacity is still occupied.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	var stdout, stderr tailBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	res := Result{StdoutTail: stdout.String(), StderrTail: stderr.String()}

	switch {
	case err == nil:
		return res, nil

	case errors.Is(runCtx.Err(), context.DeadlineExceeded):
		res.FailureClass = state.FailureTimeout
		res.Message = fmt.Sprintf("workload exceeded its %s timeout", a.Timeout)
		res.ExitCode = -1
		return res, nil

	case errors.Is(ctx.Err(), context.Canceled):
		res.FailureClass = state.FailureCanceled
		res.Message = "execution canceled"
		res.ExitCode = -1
		return res, nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		res.ExitCode = int32(exitErr.ExitCode())
		res.FailureClass = state.FailureProcessExit
		res.Message = fmt.Sprintf("workload exited with status %d", res.ExitCode)
		return res, nil
	}

	// Could not start at all: a missing binary, a bad working directory, a fork
	// failure. That is the worker's problem, not the workload's, so it is
	// retryable somewhere else.
	res.FailureClass = state.FailureSystemError
	res.Message = err.Error()
	res.ExitCode = -1
	return res, nil
}

// baseEnv is what every workload receives.
//
// ATLAS_JOB_ID is stable across retries and ATLAS_ATTEMPT_ID is unique per physical
// execution. Under at-least-once semantics those two identifiers are what a workload
// needs to make its own side effects idempotent — see docs/SEMANTICS.md §1.
func baseEnv(a scheduler.Assignment) []string {
	return []string{
		"ATLAS_JOB_ID=" + a.JobID,
		"ATLAS_ATTEMPT_ID=" + a.AttemptID,
		"ATLAS_ATTEMPT_NUMBER=" + strconv.Itoa(int(a.AttemptNumber)),
		"ATLAS_LEASE_ID=" + a.LeaseID,
		"ATLAS_CPU_MILLIS=" + strconv.FormatInt(a.Request.CPUMillis, 10),
		"ATLAS_MEMORY_BYTES=" + strconv.FormatInt(a.Request.MemoryBytes, 10),
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
}

func flattenEnv(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}

// tailBuffer keeps only the last tailBytes written to it, so a chatty workload
// cannot exhaust the worker's memory.
type tailBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := len(p)
	if len(p) > tailBytes {
		p = p[len(p)-tailBytes:]
	}
	t.buf.Write(p)
	if t.buf.Len() > tailBytes {
		excess := t.buf.Len() - tailBytes
		t.buf.Next(excess)
	}
	return n, nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buf.String()
}

// DockerExecutor runs the assignment inside a container with the requested resource
// limits actually enforced by the kernel.
//
// It shells out to the docker CLI rather than using the Engine API. That is a
// deliberate trade: the CLI is one dependency instead of a large client library, and
// Atlas uses four of its flags. If container lifecycle management ever grows beyond
// run-and-wait, the API client becomes worth its weight.
type DockerExecutor struct {
	// Binary is the docker executable; defaults to "docker".
	Binary string
	// Network is passed to --network. Defaults to "none": a batch workload that
	// needs the network should say so.
	Network string
	// PullTimeout bounds image pulls, which otherwise have no deadline of their
	// own and would be charged against the job's timeout.
	PullTimeout time.Duration
}

func (DockerExecutor) Name() string { return "docker" }

func (d DockerExecutor) binary() string {
	if d.Binary == "" {
		return "docker"
	}
	return d.Binary
}

// Available reports whether the Docker daemon can be reached, so the worker can fail
// at startup rather than failing every job.
func (d DockerExecutor) Available(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, d.binary(), "version", "--format", "{{.Server.Version}}").CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker is not usable: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (d DockerExecutor) Run(ctx context.Context, a scheduler.Assignment) (Result, error) {
	if a.Image == "" {
		return Result{
			FailureClass: state.FailureSystemError,
			Message:      "docker executor requires an image",
		}, nil
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if a.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, a.Timeout)
		defer cancel()
	}

	// The container name is the attempt id, so a leaked container is traceable
	// back to the exact execution that leaked it, and so --rm plus an explicit
	// name makes a second attempt collision-free.
	name := "atlas-" + a.AttemptID

	args := []string{
		"run", "--rm",
		"--name", name,
		"--network", d.network(),
		// Docker wants CPU as a fraction of a core; Atlas accounts in
		// millicores.
		"--cpus", strconv.FormatFloat(float64(a.Request.CPUMillis)/1000.0, 'f', 3, 64),
		"--memory", strconv.FormatInt(a.Request.MemoryBytes, 10),
	}
	for _, kv := range baseEnv(a) {
		args = append(args, "--env", kv)
	}
	for k, v := range a.Env {
		args = append(args, "--env", k+"="+v)
	}
	args = append(args, a.Image)
	args = append(args, a.Command...)

	cmd := exec.CommandContext(runCtx, d.binary(), args...)
	var stdout, stderr tailBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Cancel = func() error {
		// Killing the CLI would orphan the container, so stop the container
		// itself and let the CLI exit on its own.
		return exec.Command(d.binary(), "kill", name).Run()
	}

	err := cmd.Run()
	res := Result{StdoutTail: stdout.String(), StderrTail: stderr.String()}

	switch {
	case err == nil:
		return res, nil
	case errors.Is(runCtx.Err(), context.DeadlineExceeded):
		res.FailureClass = state.FailureTimeout
		res.Message = fmt.Sprintf("container exceeded its %s timeout", a.Timeout)
		res.ExitCode = -1
		return res, nil
	case errors.Is(ctx.Err(), context.Canceled):
		res.FailureClass = state.FailureCanceled
		res.Message = "execution canceled"
		res.ExitCode = -1
		return res, nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := int32(exitErr.ExitCode())
		res.ExitCode = code
		// `docker run` reuses 125 and 126/127 for its own failures. Those are
		// infrastructure problems (image missing, entrypoint not executable),
		// which are retryable on another worker; a workload's own non-zero exit
		// is not.
		switch code {
		case 125, 126, 127:
			res.FailureClass = state.FailureResourceError
			res.Message = "docker could not run the container: " + strings.TrimSpace(res.StderrTail)
		default:
			res.FailureClass = state.FailureProcessExit
			res.Message = fmt.Sprintf("container exited with status %d", code)
		}
		return res, nil
	}

	res.FailureClass = state.FailureSystemError
	res.Message = err.Error()
	res.ExitCode = -1
	return res, nil
}

func (d DockerExecutor) network() string {
	if d.Network == "" {
		return "none"
	}
	return d.Network
}

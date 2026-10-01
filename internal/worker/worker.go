package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/BruceMoseti/Atlas/internal/logging"
	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/types"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config describes a worker agent.
type Config struct {
	WorkerID string
	Hostname string
	Version  string
	Labels   map[string]string
	Capacity types.Resources

	Executor Executor
	Logger   *slog.Logger

	// AcquireWait is the long-poll budget per AcquireJob call.
	AcquireWait time.Duration

	// Chaos knobs. These exist so the chaos framework can inject faults from
	// inside a real worker process rather than only from outside it, which is
	// the only way to reach cases like "the response was lost but the request
	// was not".
	//
	// DuplicateRPCRate is the probability that a completion report is sent
	// twice, simulating a retry after a lost response.
	DuplicateRPCRate float64
	// HeartbeatDropRate is the probability that a heartbeat is skipped.
	HeartbeatDropRate float64
	// RenewDropRate is the probability that a lease renewal is skipped.
	RenewDropRate float64
}

func (c *Config) applyDefaults() error {
	if c.WorkerID == "" {
		return errors.New("worker: WorkerID is required")
	}
	if c.Capacity.CPUMillis <= 0 || c.Capacity.MemoryBytes <= 0 {
		return fmt.Errorf("worker: capacity must be positive (got cpu=%d mem=%d)",
			c.Capacity.CPUMillis, c.Capacity.MemoryBytes)
	}
	if c.Hostname == "" {
		c.Hostname, _ = os.Hostname()
	}
	if c.Executor == nil {
		c.Executor = ProcessExecutor{}
	}
	if c.Logger == nil {
		c.Logger = logging.Discard()
	}
	if c.AcquireWait <= 0 {
		c.AcquireWait = 5 * time.Second
	}
	return nil
}

// execution is one assignment the worker is currently running.
type execution struct {
	assignment scheduler.Assignment
	cancel     context.CancelFunc
}

// Worker is the agent. One Worker owns one connection to the scheduler and runs
// three concurrent loops: heartbeat, acquire, and lease renewal.
//
// Every network call originates here. The scheduler never connects to a worker, so
// workers can live behind NAT and the control plane needs no service discovery.
type Worker struct {
	cfg    Config
	client pb.WorkerServiceClient
	log    *slog.Logger

	generation         int64
	heartbeatInterval  time.Duration
	leaseRenewInterval time.Duration

	mu     sync.Mutex
	active map[string]*execution

	wg sync.WaitGroup
}

// New builds a worker over an established gRPC client.
func New(client pb.WorkerServiceClient, cfg Config) (*Worker, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	return &Worker{
		cfg:    cfg,
		client: client,
		log:    cfg.Logger.With("worker_id", cfg.WorkerID),
		active: make(map[string]*execution),
	}, nil
}

// Run registers and then serves until ctx is canceled. It returns once every
// in-flight execution has been torn down.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.register(ctx); err != nil {
		return err
	}

	loops := []func(context.Context){w.heartbeatLoop, w.acquireLoop, w.renewLoop}
	w.wg.Add(len(loops))
	for _, loop := range loops {
		go func(f func(context.Context)) { defer w.wg.Done(); f(ctx) }(loop)
	}
	w.wg.Wait()

	// Cancelling the executions releases their processes. Their leases will
	// expire and the scheduler will retry them elsewhere; a worker shutting down
	// cannot promise anything better under at-least-once semantics.
	w.cancelAll("worker shutting down")
	return nil
}

func (w *Worker) register(ctx context.Context) error {
	res, err := w.client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: w.cfg.WorkerID,
		Hostname: w.cfg.Hostname,
		Version:  w.cfg.Version,
		Labels:   w.cfg.Labels,
		Capacity: &pb.ResourceSpec{
			CpuMillis:   w.cfg.Capacity.CPUMillis,
			MemoryBytes: w.cfg.Capacity.MemoryBytes,
		},
	})
	if err != nil {
		return fmt.Errorf("worker: register: %w", err)
	}

	w.generation = res.Generation
	w.heartbeatInterval = time.Duration(res.HeartbeatIntervalMillis) * time.Millisecond
	w.leaseRenewInterval = time.Duration(res.LeaseRenewIntervalMillis) * time.Millisecond
	if w.heartbeatInterval <= 0 {
		w.heartbeatInterval = 2 * time.Second
	}
	if w.leaseRenewInterval <= 0 {
		w.leaseRenewInterval = 3 * time.Second
	}

	w.log.Info("worker_registered",
		"generation", w.generation,
		"executor", w.cfg.Executor.Name(),
		"cpu_millis", w.cfg.Capacity.CPUMillis,
		"memory_bytes", w.cfg.Capacity.MemoryBytes,
		"heartbeat_ms", w.heartbeatInterval.Milliseconds(),
		"lease_renew_ms", w.leaseRenewInterval.Milliseconds())
	return nil
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if chance(w.cfg.HeartbeatDropRate) {
			continue
		}

		res, err := w.client.Heartbeat(ctx, &pb.HeartbeatRequest{
			WorkerId:         w.cfg.WorkerID,
			Generation:       w.generation,
			ActiveAttemptIds: w.activeAttemptIDs(),
		})
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("heartbeat_failed", "error", err)
			}
			continue
		}
		if res.MustReregister {
			w.log.Warn("heartbeat_rejected", "reason", "scheduler does not recognize this worker")
			if err := w.register(ctx); err != nil && ctx.Err() == nil {
				w.log.Error("reregistration_failed", "error", err)
			}
			continue
		}
		for _, id := range res.CancelAttemptIds {
			w.cancelAttempt(id, "scheduler reclaimed this attempt")
		}
	}
}

// acquireLoop long-polls for work. It stops pulling when the worker is at capacity
// locally, though the scheduler's own accounting is what actually prevents
// oversubscription.
func (w *Worker) acquireLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		res, err := w.client.AcquireJob(ctx, &pb.AcquireJobRequest{
			WorkerId:   w.cfg.WorkerID,
			Generation: w.generation,
			WaitMillis: w.cfg.AcquireWait.Milliseconds(),
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if status.Code(err) == codes.FailedPrecondition {
				// The scheduler lost us, or we are running on stale
				// credentials. Registering again is the only way forward.
				w.log.Warn("acquire_rejected", "error", err)
				if rerr := w.register(ctx); rerr != nil && ctx.Err() == nil {
					w.log.Error("reregistration_failed", "error", rerr)
					sleep(ctx, time.Second)
				}
				continue
			}
			w.log.Warn("acquire_failed", "error", err)
			sleep(ctx, 500*time.Millisecond)
			continue
		}
		for _, pbAssignment := range res.Assignments {
			w.startExecution(ctx, assignmentFromPB(pbAssignment))
		}
	}
}

// renewLoop keeps every active lease alive. A worker that cannot renew loses its
// assignments, which is the intended behaviour: an unreachable worker must not hold
// capacity indefinitely.
func (w *Worker) renewLoop(ctx context.Context) {
	ticker := time.NewTicker(w.leaseRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if chance(w.cfg.RenewDropRate) {
			continue
		}

		for _, ex := range w.snapshotActive() {
			res, err := w.client.RenewLease(ctx, &pb.RenewLeaseRequest{Ref: w.ref(ex.assignment)})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.NotFound {
					// Our authority is gone. Killing the execution
					// immediately is what keeps a reclaimed job from running
					// twice for longer than it has to.
					w.cancelAttempt(ex.assignment.AttemptID, "lease is no longer valid")
					continue
				}
				w.log.Warn("renew_failed", "job_id", ex.assignment.JobID,
					"attempt_id", ex.assignment.AttemptID, "error", err)
				continue
			}
			if res.Canceled {
				w.cancelAttempt(ex.assignment.AttemptID, "job canceled")
			}
		}
	}
}

func (w *Worker) startExecution(ctx context.Context, a scheduler.Assignment) {
	execCtx, cancel := context.WithCancel(ctx)

	w.mu.Lock()
	if _, dup := w.active[a.AttemptID]; dup {
		w.mu.Unlock()
		cancel()
		return
	}
	w.active[a.AttemptID] = &execution{assignment: a, cancel: cancel}
	w.mu.Unlock()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer cancel()
		w.execute(ctx, execCtx, a)
		w.mu.Lock()
		delete(w.active, a.AttemptID)
		w.mu.Unlock()
	}()
}

// execute runs one assignment and reports the outcome.
//
// reportCtx is deliberately separate from execCtx: when an execution is canceled we
// still want to tell the scheduler what happened, using a context that is not
// already dead.
func (w *Worker) execute(reportCtx, execCtx context.Context, a scheduler.Assignment) {
	ref := w.ref(a)

	if _, err := w.client.StartJob(reportCtx, &pb.StartJobRequest{Ref: ref}); err != nil {
		if status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.NotFound {
			w.log.Warn("start_rejected", "job_id", a.JobID, "attempt_id", a.AttemptID, "error", err)
			return
		}
		// Any other error is transient. Run the workload anyway: the lease is
		// still ours, and refusing to work because one RPC failed would be
		// worse than a missing START record.
		w.log.Warn("start_failed", "job_id", a.JobID, "attempt_id", a.AttemptID, "error", err)
	}

	w.log.Info("execution_started",
		"job_id", a.JobID, "attempt_id", a.AttemptID, "attempt", a.AttemptNumber,
		"lease_id", a.LeaseID, "image", a.Image)

	started := time.Now()
	res, err := w.cfg.Executor.Run(execCtx, a)
	if err != nil {
		res = Result{FailureClass: state.FailureSystemError, Message: err.Error(), ExitCode: -1}
	}

	// Reporting must not inherit a canceled execution context, and must not hang
	// forever either.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(reportCtx), 15*time.Second)
	defer cancel()

	if res.Succeeded() {
		w.report(ctx, func(c context.Context) error {
			_, err := w.client.CompleteJob(c, &pb.CompleteJobRequest{
				Ref:        ref,
				ExitCode:   res.ExitCode,
				StdoutTail: res.StdoutTail,
				StderrTail: res.StderrTail,
			})
			return err
		}, "CompleteJob", a)
		w.log.Info("execution_finished",
			"job_id", a.JobID, "attempt_id", a.AttemptID, "exit_code", res.ExitCode,
			"duration_ms", time.Since(started).Milliseconds())
		return
	}

	w.report(ctx, func(c context.Context) error {
		_, err := w.client.FailJob(c, &pb.FailJobRequest{
			Ref:          ref,
			FailureClass: failureClassToPB(res.FailureClass),
			Message:      res.Message,
			ExitCode:     res.ExitCode,
			HasExitCode:  res.ExitCode >= 0,
			StdoutTail:   res.StdoutTail,
			StderrTail:   res.StderrTail,
		})
		return err
	}, "FailJob", a)
	w.log.Warn("execution_failed",
		"job_id", a.JobID, "attempt_id", a.AttemptID,
		"failure_class", string(res.FailureClass), "exit_code", res.ExitCode,
		"duration_ms", time.Since(started).Milliseconds(), "message", res.Message)
}

// report sends a completion report, retrying transient failures.
//
// Retrying is only safe because the scheduler makes these RPCs idempotent per
// attempt; without that guarantee a lost response would force the worker to choose
// between losing the result and corrupting it.
func (w *Worker) report(ctx context.Context, send func(context.Context) error, rpc string, a scheduler.Assignment) {
	const attempts = 5
	backoff := 100 * time.Millisecond

	for i := 0; i < attempts; i++ {
		err := send(ctx)
		if err == nil {
			if chance(w.cfg.DuplicateRPCRate) {
				// Deliberately replay the report, imitating a retry after a
				// lost response. The scheduler must answer "already recorded"
				// rather than applying it twice.
				_ = send(ctx)
			}
			return
		}
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.NotFound, codes.InvalidArgument:
			// The scheduler rejected us on the merits. Retrying cannot help.
			w.log.Warn("report_rejected", "rpc", rpc, "job_id", a.JobID,
				"attempt_id", a.AttemptID, "error", err)
			return
		}
		if ctx.Err() != nil {
			return
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	w.log.Error("report_abandoned", "rpc", rpc, "job_id", a.JobID, "attempt_id", a.AttemptID)
}

func (w *Worker) ref(a scheduler.Assignment) *pb.AttemptRef {
	return &pb.AttemptRef{
		JobId:     a.JobID,
		AttemptId: a.AttemptID,
		LeaseId:   a.LeaseID,
		WorkerId:  w.cfg.WorkerID,
	}
}

func (w *Worker) activeAttemptIDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.active))
	for id := range w.active {
		out = append(out, id)
	}
	return out
}

func (w *Worker) snapshotActive() []*execution {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*execution, 0, len(w.active))
	for _, ex := range w.active {
		out = append(out, ex)
	}
	return out
}

func (w *Worker) cancelAttempt(attemptID, reason string) {
	w.mu.Lock()
	ex, ok := w.active[attemptID]
	w.mu.Unlock()
	if !ok {
		return
	}
	w.log.Warn("execution_canceled", "attempt_id", attemptID,
		"job_id", ex.assignment.JobID, "reason", reason)
	ex.cancel()
}

func (w *Worker) cancelAll(reason string) {
	for _, ex := range w.snapshotActive() {
		w.log.Warn("execution_canceled", "attempt_id", ex.assignment.AttemptID,
			"job_id", ex.assignment.JobID, "reason", reason)
		ex.cancel()
	}
}

// ActiveCount is how many executions are running right now.
func (w *Worker) ActiveCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.active)
}

func assignmentFromPB(a *pb.Assignment) scheduler.Assignment {
	out := scheduler.Assignment{
		JobID:          a.JobId,
		AttemptID:      a.AttemptId,
		AttemptNumber:  a.AttemptNumber,
		LeaseID:        a.LeaseId,
		LeaseExpiresAt: time.Unix(0, a.LeaseExpiresAtUnixNanos),
		Image:          a.Image,
		Command:        a.Command,
		Env:            a.Env,
		Timeout:        time.Duration(a.TimeoutSeconds) * time.Second,
	}
	if a.Request != nil {
		out.Request = types.Resources{CPUMillis: a.Request.CpuMillis, MemoryBytes: a.Request.MemoryBytes}
	}
	return out
}

func failureClassToPB(c state.FailureClass) pb.FailureClass {
	switch c {
	case state.FailureProcessExit:
		return pb.FailureClass_FAILURE_CLASS_PROCESS_EXIT
	case state.FailureTimeout:
		return pb.FailureClass_FAILURE_CLASS_TIMEOUT
	case state.FailureWorkerLost:
		return pb.FailureClass_FAILURE_CLASS_WORKER_LOST
	case state.FailureResourceError:
		return pb.FailureClass_FAILURE_CLASS_RESOURCE_ERROR
	case state.FailureCanceled:
		return pb.FailureClass_FAILURE_CLASS_CANCELED
	default:
		return pb.FailureClass_FAILURE_CLASS_SYSTEM_ERROR
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

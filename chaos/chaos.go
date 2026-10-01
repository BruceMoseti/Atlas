// Package chaos runs randomized fault-injection campaigns against a real Atlas
// cluster and then checks that the documented invariants still hold.
//
// Everything here is a real process. The scheduler is a real atlas-server with a
// real SQLite database on disk; the workers are real atlas-worker processes
// executing real commands. Faults are delivered the way they happen in production:
// SIGKILL, SIGSTOP, a restart, a dropped heartbeat, a replayed RPC.
//
// That matters because the properties under test only exist in the presence of real
// concurrency and real partial failure. A mocked worker that politely reports its
// own death proves nothing about what happens when a machine stops answering.
package chaos

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/BruceMoseti/Atlas/internal/invariants"
	"github.com/BruceMoseti/Atlas/internal/store"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config describes a campaign.
type Config struct {
	// BinDir holds atlas-server and atlas-worker.
	BinDir string
	// WorkDir holds the database, logs, and the report. Created if missing.
	WorkDir string

	GRPCAddr string
	HTTPAddr string

	Workers int
	Jobs    int
	// Duration bounds the fault-injection phase. The campaign then stops
	// injecting and waits for the cluster to drain.
	Duration time.Duration
	// DrainTimeout bounds the quiescent phase after faults stop.
	DrainTimeout time.Duration

	// Fault toggles.
	KillWorkers       bool
	PauseWorkers      bool
	RestartScheduler  bool
	DuplicateRPCRate  float64
	HeartbeatDropRate float64
	RenewDropRate     float64

	// FaultInterval is the mean time between injected faults.
	FaultInterval time.Duration

	// Scheduler tuning, passed through to atlas-server.
	LeaseTTL     time.Duration
	SuspectAfter time.Duration
	DeadAfter    time.Duration

	// JobDuration is how long each submitted workload sleeps.
	JobDuration time.Duration
	// MaxAttempts is the attempt budget given to every job.
	MaxAttempts int

	Seed int64
	// Verbose echoes child process output to stderr.
	Verbose bool
}

func (c *Config) applyDefaults() error {
	if c.BinDir == "" {
		c.BinDir = "bin"
	}
	for _, bin := range []string{"atlas-server", "atlas-worker"} {
		if _, err := os.Stat(filepath.Join(c.BinDir, bin)); err != nil {
			return fmt.Errorf("chaos: %s not found in %s (run `make build` first): %w", bin, c.BinDir, err)
		}
	}
	if c.WorkDir == "" {
		c.WorkDir = filepath.Join("results", "chaos-"+time.Now().UTC().Format("20060102-150405"))
	}
	if err := os.MkdirAll(c.WorkDir, 0o755); err != nil {
		return err
	}
	if c.GRPCAddr == "" {
		c.GRPCAddr = "127.0.0.1:50551"
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = "127.0.0.1:59090"
	}
	if c.Workers <= 0 {
		c.Workers = 6
	}
	if c.Jobs <= 0 {
		c.Jobs = 500
	}
	if c.Duration <= 0 {
		c.Duration = 60 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 3 * time.Minute
	}
	if c.FaultInterval <= 0 {
		c.FaultInterval = 3 * time.Second
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 4 * time.Second
	}
	if c.SuspectAfter <= 0 {
		c.SuspectAfter = 1500 * time.Millisecond
	}
	if c.DeadAfter <= 0 {
		c.DeadAfter = 4 * time.Second
	}
	if c.JobDuration <= 0 {
		c.JobDuration = 300 * time.Millisecond
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}
	return nil
}

// Report is what a campaign produced.
type Report struct {
	Config Config

	Started  time.Time
	Finished time.Time

	JobsSubmitted  int
	JobsAccepted   int
	JobsRejected   int
	SubmitFailures int

	Faults         []Fault
	FaultsByKind   map[string]int
	InvariantsRep  *invariants.Report
	AcceptedJobIDs []string

	// RecoveryLatencies are the observed gaps between a job's attempt being
	// reclaimed and its next attempt starting, scraped from the attempt
	// records.
	RecoveryP50 time.Duration
	RecoveryP99 time.Duration
	RecoveryMax time.Duration

	Drained bool
}

// Fault is one injected failure.
type Fault struct {
	At     time.Duration
	Kind   string
	Target string
}

// Run executes a campaign end to end: start the cluster, submit work, inject faults,
// let it drain, then check every invariant.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}

	c := &campaign{
		cfg:    cfg,
		rng:    rand.New(rand.NewPCG(uint64(cfg.Seed), 0x5eed)),
		report: &Report{Config: cfg, Started: time.Now(), FaultsByKind: map[string]int{}},
	}
	defer c.shutdown()

	if err := c.startScheduler(ctx); err != nil {
		return nil, err
	}
	if err := c.connect(ctx); err != nil {
		return nil, err
	}
	for i := 0; i < cfg.Workers; i++ {
		if err := c.startWorker(ctx, fmt.Sprintf("w%d", i)); err != nil {
			return nil, err
		}
	}
	if err := c.waitForFleet(ctx, cfg.Workers, 30*time.Second); err != nil {
		return nil, err
	}

	submitDone := make(chan struct{})
	go func() { defer close(submitDone); c.submitLoop(ctx) }()

	c.injectFaults(ctx)
	<-submitDone

	c.report.Drained = c.waitForDrain(ctx, cfg.DrainTimeout)
	c.report.Finished = time.Now()

	// Stop everything before reading the database, so the invariant check sees
	// a quiescent state rather than one mid-transaction.
	c.shutdown()

	if err := c.check(ctx); err != nil {
		return c.report, err
	}
	return c.report, nil
}

type managedProc struct {
	name string
	cmd  *exec.Cmd
	log  *os.File
}

type campaign struct {
	cfg    Config
	rng    *rand.Rand
	report *Report

	conn         *grpc.ClientConn
	client       pb.AtlasServiceClient
	scheduler    *managedProc
	mu           sync.Mutex
	workers      map[string]*managedProc
	pausedWorker map[string]bool
	shutdownOnce sync.Once
}

func (c *campaign) dbPath() string { return filepath.Join(c.cfg.WorkDir, "atlas.db") }

func (c *campaign) startScheduler(ctx context.Context) error {
	args := []string{
		"--listen", c.cfg.GRPCAddr,
		"--http", c.cfg.HTTPAddr,
		"--db", c.dbPath(),
		"--lease-ttl", c.cfg.LeaseTTL.String(),
		"--heartbeat-interval", "300ms",
		"--suspect-after", c.cfg.SuspectAfter.String(),
		"--dead-after", c.cfg.DeadAfter.String(),
		"--reconcile-interval", "150ms",
		"--retry-base-delay", "100ms",
		"--retry-max-delay", "2s",
		"--max-queue-depth", "200000",
		"--log-level", "warn",
	}
	p, err := c.spawn(ctx, "scheduler", filepath.Join(c.cfg.BinDir, "atlas-server"), args)
	if err != nil {
		return err
	}
	c.scheduler = p
	return nil
}

func (c *campaign) startWorker(ctx context.Context, id string) error {
	args := []string{
		"--scheduler", c.cfg.GRPCAddr,
		"--id", id,
		"--cpu", "4",
		"--memory", "4GB",
		"--acquire-wait", "300ms",
		"--log-level", "warn",
	}
	if c.cfg.DuplicateRPCRate > 0 {
		args = append(args, "--chaos-duplicate-rpc-rate", fmt.Sprint(c.cfg.DuplicateRPCRate))
	}
	if c.cfg.HeartbeatDropRate > 0 {
		args = append(args, "--chaos-heartbeat-drop-rate", fmt.Sprint(c.cfg.HeartbeatDropRate))
	}
	if c.cfg.RenewDropRate > 0 {
		args = append(args, "--chaos-renew-drop-rate", fmt.Sprint(c.cfg.RenewDropRate))
	}

	p, err := c.spawn(ctx, "worker-"+id, filepath.Join(c.cfg.BinDir, "atlas-worker"), args)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.workers == nil {
		c.workers = map[string]*managedProc{}
		c.pausedWorker = map[string]bool{}
	}
	c.workers[id] = p
	delete(c.pausedWorker, id)
	c.mu.Unlock()
	return nil
}

// spawn starts a child in its own process group, so that killing it takes anything
// it started with it.
func (c *campaign) spawn(ctx context.Context, name, bin string, args []string) (*managedProc, error) {
	logPath := filepath.Join(c.cfg.WorkDir, name+".log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(bin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var out io.Writer = f
	if c.cfg.Verbose {
		out = io.MultiWriter(f, prefixWriter{prefix: name + ": ", w: os.Stderr})
	}
	cmd.Stdout = out
	cmd.Stderr = out

	if err := cmd.Start(); err != nil {
		f.Close()
		return nil, fmt.Errorf("chaos: start %s: %w", name, err)
	}
	return &managedProc{name: name, cmd: cmd, log: f}, nil
}

// kill delivers SIGKILL to the whole process group. No cleanup, no final report:
// this is what a machine losing power looks like to the scheduler.
func (p *managedProc) kill() {
	if p == nil || p.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	_, _ = p.cmd.Process.Wait()
	if p.log != nil {
		_ = p.log.Close()
	}
}

func (p *managedProc) signal(sig syscall.Signal) {
	if p == nil || p.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-p.cmd.Process.Pid, sig)
}

func (c *campaign) connect(ctx context.Context) error {
	conn, err := grpc.NewClient(c.cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.conn = conn
	c.client = pb.NewAtlasServiceClient(conn)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.client.GetClusterStatus(ctx, &pb.ClusterStatusRequest{}); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errors.New("chaos: scheduler did not become reachable")
}

func (c *campaign) waitForFleet(ctx context.Context, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := c.client.ListWorkers(ctx, &pb.ListWorkersRequest{})
		if err == nil {
			healthy := 0
			for _, w := range res.Workers {
				if w.State == pb.WorkerState_WORKER_STATE_HEALTHY {
					healthy++
				}
			}
			if healthy >= want {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("chaos: only part of the fleet registered within %s", timeout)
}

// submitLoop submits the campaign's jobs, spread over the fault-injection window so
// that submissions and faults overlap.
func (c *campaign) submitLoop(ctx context.Context) {
	interval := c.cfg.Duration / time.Duration(c.cfg.Jobs+1)
	sleepCmd := fmt.Sprintf("sleep %.3f", c.cfg.JobDuration.Seconds())

	for i := 0; i < c.cfg.Jobs; i++ {
		if ctx.Err() != nil {
			return
		}
		req := &pb.SubmitJobRequest{
			// A deterministic key per job, so a submission the client retries
			// after a timeout deduplicates rather than creating a second job.
			IdempotencyKey: fmt.Sprintf("chaos-%d-%d", c.cfg.Seed, i),
			ClientId:       "chaos",
			Command:        []string{"sh", "-c", sleepCmd},
			CpuMillis:      1000,
			MemoryBytes:    512 << 20,
			MaxAttempts:    int32(c.cfg.MaxAttempts),
			Priority:       int32(c.rng.IntN(4) * 10),
		}

		c.report.JobsSubmitted++
		res, err := c.submitWithRetry(ctx, req)
		switch {
		case err != nil:
			c.report.SubmitFailures++
		case res == nil:
			c.report.JobsRejected++
		default:
			c.report.JobsAccepted++
			c.report.AcceptedJobIDs = append(c.report.AcceptedJobIDs, res.JobId)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// submitWithRetry retries a submission the way a real client would. It is safe
// precisely because submission is idempotent: a retry after an ambiguous failure
// returns the original job rather than creating a second one.
func (c *campaign) submitWithRetry(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		res, err := c.client.SubmitJob(callCtx, req)
		cancel()
		if err == nil {
			return res, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// The scheduler is probably restarting. Wait and try again; that is
		// exactly the behaviour idempotent submission is designed to support.
		time.Sleep(300 * time.Millisecond)
	}
	return nil, lastErr
}

// injectFaults is the campaign's fault schedule.
func (c *campaign) injectFaults(ctx context.Context) {
	start := time.Now()
	deadline := start.Add(c.cfg.Duration)

	var kinds []string
	if c.cfg.KillWorkers {
		kinds = append(kinds, "kill-worker")
	}
	if c.cfg.PauseWorkers {
		kinds = append(kinds, "pause-worker")
	}
	if c.cfg.RestartScheduler {
		kinds = append(kinds, "restart-scheduler")
	}
	if len(kinds) == 0 {
		// No process-level faults, but the worker-level chaos rates are still
		// in effect for the whole window.
		select {
		case <-ctx.Done():
		case <-time.After(c.cfg.Duration):
		}
		return
	}

	for time.Now().Before(deadline) {
		wait := time.Duration(c.rng.Float64() * 2 * float64(c.cfg.FaultInterval))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if time.Now().After(deadline) {
			return
		}

		kind := kinds[c.rng.IntN(len(kinds))]
		// A scheduler restart is disruptive enough that doing it as often as a
		// worker kill would leave no steady state to disturb.
		if kind == "restart-scheduler" && c.rng.Float64() > 0.25 {
			continue
		}

		target := c.applyFault(ctx, kind)
		f := Fault{At: time.Since(start), Kind: kind, Target: target}
		c.report.Faults = append(c.report.Faults, f)
		c.report.FaultsByKind[kind]++
	}
}

func (c *campaign) applyFault(ctx context.Context, kind string) string {
	switch kind {
	case "kill-worker":
		id, p := c.pickWorker()
		if p == nil {
			return ""
		}
		p.kill()
		c.mu.Lock()
		delete(c.workers, id)
		c.mu.Unlock()
		// Bring it back shortly, so the fleet does not simply shrink to
		// nothing over a long campaign.
		go func() {
			time.Sleep(time.Duration(500+c.rng.IntN(2000)) * time.Millisecond)
			if ctx.Err() == nil {
				_ = c.startWorker(ctx, id)
			}
		}()
		return id

	case "pause-worker":
		// SIGSTOP is the cruellest fault: the worker is alive, holds its
		// leases, and answers nothing. It is what a long GC pause or a frozen
		// hypervisor looks like.
		id, p := c.pickWorker()
		if p == nil {
			return ""
		}
		c.mu.Lock()
		if c.pausedWorker[id] {
			c.mu.Unlock()
			return ""
		}
		c.pausedWorker[id] = true
		c.mu.Unlock()

		p.signal(syscall.SIGSTOP)
		go func() {
			time.Sleep(time.Duration(1000+c.rng.IntN(3000)) * time.Millisecond)
			p.signal(syscall.SIGCONT)
			c.mu.Lock()
			delete(c.pausedWorker, id)
			c.mu.Unlock()
		}()
		return id

	case "restart-scheduler":
		c.scheduler.kill()
		time.Sleep(time.Duration(200+c.rng.IntN(800)) * time.Millisecond)
		if err := c.startScheduler(ctx); err != nil {
			return "failed: " + err.Error()
		}
		return "scheduler"
	}
	return ""
}

func (c *campaign) pickWorker() (string, *managedProc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.workers))
	for id := range c.workers {
		if !c.pausedWorker[id] {
			ids = append(ids, id)
		}
	}
	// Never take the last worker away: a cluster with no capacity cannot make
	// progress, and the campaign would be measuring a timeout rather than
	// recovery.
	if len(ids) <= 1 {
		return "", nil
	}
	sort.Strings(ids)
	id := ids[c.rng.IntN(len(ids))]
	return id, c.workers[id]
}

// waitForDrain waits for every accepted job to reach a terminal state.
func (c *campaign) waitForDrain(ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		st, err := c.client.GetClusterStatus(ctx, &pb.ClusterStatusRequest{})
		if err == nil && st.JobsQueued == 0 && st.JobsRunning == 0 {
			// Confirm with a second reading: a momentary zero can happen
			// between a completion and the next dispatch.
			time.Sleep(500 * time.Millisecond)
			st2, err2 := c.client.GetClusterStatus(ctx, &pb.ClusterStatusRequest{})
			if err2 == nil && st2.JobsQueued == 0 && st2.JobsRunning == 0 {
				return true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// check runs the invariant suite against the database the campaign produced.
func (c *campaign) check(ctx context.Context) error {
	st, err := store.Open(store.Options{Path: c.dbPath()})
	if err != nil {
		return fmt.Errorf("chaos: reopen store: %w", err)
	}
	defer st.Close()

	rep, err := invariants.Check(ctx, st)
	if err != nil {
		return err
	}
	missing, err := invariants.CheckAcceptedJobs(ctx, st, c.report.AcceptedJobIDs)
	if err != nil {
		return err
	}
	rep.Violations = append(rep.Violations, missing...)
	c.report.InvariantsRep = rep

	p50, p99, max, err := recoveryLatencies(ctx, st)
	if err != nil {
		return err
	}
	c.report.RecoveryP50, c.report.RecoveryP99, c.report.RecoveryMax = p50, p99, max
	return nil
}

// recoveryLatencies measures how long each reclaimed attempt took to be replaced,
// by pairing every LOST attempt with the next attempt on the same job.
//
// This is the number that says how quickly Atlas notices a failure and acts on it,
// and it is derived from the durable record rather than from instrumentation, so it
// survives the scheduler restarts the campaign injects.
func recoveryLatencies(ctx context.Context, st *store.Store) (p50, p99, max time.Duration, err error) {
	var gaps []time.Duration

	err = st.View(ctx, func(tx *store.Tx) error {
		jobs, err := tx.ListJobs(store.JobFilter{})
		if err != nil {
			return err
		}
		for _, j := range jobs {
			attempts, err := tx.ListAttemptsForJob(j.ID)
			if err != nil {
				return err
			}
			for i := 0; i+1 < len(attempts); i++ {
				prev, next := attempts[i], attempts[i+1]
				if prev.State != "LOST" || prev.FinishedAt == nil {
					continue
				}
				if gap := next.CreatedAt.Sub(*prev.FinishedAt); gap >= 0 {
					gaps = append(gaps, gap)
				}
			}
		}
		return nil
	})
	if err != nil || len(gaps) == 0 {
		return 0, 0, 0, err
	}

	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	return gaps[len(gaps)*50/100], gaps[min(len(gaps)-1, len(gaps)*99/100)], gaps[len(gaps)-1], nil
}

func (c *campaign) shutdown() {
	c.shutdownOnce.Do(func() {
		c.mu.Lock()
		workers := make([]*managedProc, 0, len(c.workers))
		for _, p := range c.workers {
			workers = append(workers, p)
		}
		c.workers = map[string]*managedProc{}
		c.mu.Unlock()

		for _, p := range workers {
			p.signal(syscall.SIGCONT) // a paused process cannot handle SIGKILL
			p.kill()
		}
		if c.scheduler != nil {
			c.scheduler.kill()
		}
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

type prefixWriter struct {
	prefix string
	w      io.Writer
}

func (p prefixWriter) Write(b []byte) (int, error) {
	sc := bufio.NewScanner(newBytesReader(b))
	for sc.Scan() {
		fmt.Fprintf(p.w, "%s%s\n", p.prefix, sc.Text())
	}
	return len(b), nil
}

func newBytesReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct {
	b []byte
	i int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

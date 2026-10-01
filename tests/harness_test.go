//go:build integration

// Package tests contains Atlas's end-to-end tests.
//
// They run a real gRPC server, a real SQLite database, and real worker agents
// executing real processes. Nothing is stubbed, because the properties under test —
// lease reclamation, scheduler restart, duplicate RPCs — are exactly the ones that a
// stub would define away.
//
// Build-tagged `integration` so that `go test ./...` stays fast.
package tests

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/api"
	"github.com/BruceMoseti/Atlas/internal/invariants"
	"github.com/BruceMoseti/Atlas/internal/logging"
	"github.com/BruceMoseti/Atlas/internal/metrics"
	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
	"github.com/BruceMoseti/Atlas/internal/worker"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cluster is a running Atlas: one scheduler process worth of state, served over a
// real TCP socket, plus however many worker agents a test starts.
type cluster struct {
	t      *testing.T
	dbPath string
	cfg    scheduler.Config

	store  *store.Store
	sched  *scheduler.Scheduler
	grpc   *grpc.Server
	addr   string
	cancel context.CancelFunc

	conn   *grpc.ClientConn
	client pb.AtlasServiceClient

	mu      sync.Mutex
	workers map[string]*testWorker
}

type testWorker struct {
	id     string
	conn   *grpc.ClientConn
	agent  *worker.Worker
	cancel context.CancelFunc
	done   chan struct{}
}

// startCluster brings up a scheduler on a random loopback port.
func startCluster(t *testing.T, cfg scheduler.Config) *cluster {
	t.Helper()

	if cfg.Logger == nil {
		cfg.Logger = logging.Discard()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewNop()
	}
	c := &cluster{
		t:       t,
		dbPath:  filepath.Join(t.TempDir(), "atlas.db"),
		cfg:     cfg,
		addr:    "127.0.0.1:0",
		workers: map[string]*testWorker{},
	}
	c.bringUpControlPlane()

	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.conn = conn
	c.client = pb.NewAtlasServiceClient(conn)

	t.Cleanup(c.stop)
	return c
}

// bringUpControlPlane opens the database, starts a scheduler on it, and serves the
// gRPC API at c.addr. After the first call, c.addr is a concrete host:port, so a
// restart reuses the same socket and existing worker connections simply reconnect.
func (c *cluster) bringUpControlPlane() {
	c.t.Helper()

	st, err := store.Open(store.Options{Path: c.dbPath})
	if err != nil {
		c.t.Fatalf("open store: %v", err)
	}
	sched := scheduler.New(st, c.cfg)

	ctx, cancel := context.WithCancel(context.Background())
	if err := sched.Start(ctx); err != nil {
		cancel()
		st.Close()
		c.t.Fatalf("start scheduler: %v", err)
	}

	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		cancel()
		sched.Stop()
		st.Close()
		c.t.Fatalf("listen on %s: %v", c.addr, err)
	}
	srv := grpc.NewServer()
	s := api.NewServer(sched)
	pb.RegisterAtlasServiceServer(srv, s)
	pb.RegisterWorkerServiceServer(srv, s)
	go func() { _ = srv.Serve(lis) }()

	c.store, c.sched, c.grpc, c.cancel = st, sched, srv, cancel
	c.addr = lis.Addr().String()
}

// crashScheduler takes the control plane away without warning. Worker agents keep
// running and keep trying; their RPCs just start failing, which is what they would
// see if the scheduler's machine had gone down.
func (c *cluster) crashScheduler() {
	c.grpc.Stop()
	c.cancel()
	c.sched.Stop()
	_ = c.store.Close()
}

// restartScheduler crashes the control plane, waits, and brings a fresh one up on
// the same address and the same database. Worker agents run throughout, which is
// what makes this a restart rather than a fresh cluster.
//
// during, if given, runs while the control plane is down and receives the database
// path, for tests that need to disturb the durable state a crash left behind.
func (c *cluster) restartScheduler(downtime time.Duration, during ...func(dbPath string)) {
	c.t.Helper()
	c.crashScheduler()
	for _, fn := range during {
		fn(c.dbPath)
	}
	time.Sleep(downtime)
	c.bringUpControlPlane()
}

// stop tears the cluster down the way a clean shutdown would.
func (c *cluster) stop() {
	c.mu.Lock()
	ws := make([]*testWorker, 0, len(c.workers))
	for _, w := range c.workers {
		ws = append(ws, w)
	}
	c.workers = map[string]*testWorker{}
	c.mu.Unlock()

	for _, w := range ws {
		w.cancel()
		<-w.done
		_ = w.conn.Close()
	}
	_ = c.conn.Close()
	c.crashScheduler()
}

// startWorker adds a worker agent to the fleet.
func (c *cluster) startWorker(id string, cpuMillis, memBytes int64, opts ...func(*worker.Config)) *testWorker {
	c.t.Helper()

	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatalf("worker %s dial: %v", id, err)
	}
	cfg := worker.Config{
		WorkerID:    id,
		Version:     "test",
		Capacity:    types.Resources{CPUMillis: cpuMillis, MemoryBytes: memBytes},
		Executor:    worker.ProcessExecutor{},
		Logger:      logging.Discard(),
		AcquireWait: 500 * time.Millisecond,
	}
	for _, o := range opts {
		o(&cfg)
	}

	agent, err := worker.New(pb.NewWorkerServiceClient(conn), cfg)
	if err != nil {
		c.t.Fatalf("worker %s: %v", id, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	tw := &testWorker{id: id, conn: conn, agent: agent, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(tw.done)
		_ = agent.Run(ctx)
	}()

	c.mu.Lock()
	c.workers[id] = tw
	c.mu.Unlock()

	c.waitForWorker(id, 10*time.Second)
	return tw
}

// killWorker makes a worker vanish without telling the scheduler. Its connection is
// closed first so that in-flight RPCs fail the way they would if the machine had
// gone away, rather than draining politely.
func (c *cluster) killWorker(id string) {
	c.t.Helper()
	c.mu.Lock()
	tw, ok := c.workers[id]
	delete(c.workers, id)
	c.mu.Unlock()
	if !ok {
		c.t.Fatalf("no such worker %q", id)
	}
	_ = tw.conn.Close()
	tw.cancel()
	<-tw.done
}

func (c *cluster) waitForWorker(id string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := c.client.ListWorkers(context.Background(), &pb.ListWorkersRequest{})
		if err == nil {
			for _, w := range res.Workers {
				if w.WorkerId == id && w.State == pb.WorkerState_WORKER_STATE_HEALTHY {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("worker %s did not become healthy within %s", id, timeout)
}

// submit sends a job and fails the test if the scheduler refuses it.
func (c *cluster) submit(req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	c.t.Helper()
	if req.CpuMillis == 0 {
		req.CpuMillis = 100
	}
	if req.MemoryBytes == 0 {
		req.MemoryBytes = 16 << 20
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 3
	}
	res, err := c.client.SubmitJob(context.Background(), req)
	if err != nil {
		c.t.Fatalf("submit: %v", err)
	}
	return res
}

// submitN submits count identical shell jobs and returns their ids.
func (c *cluster) submitN(count int, command []string, cpuMillis, memBytes int64) []string {
	c.t.Helper()
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		res := c.submit(&pb.SubmitJobRequest{
			IdempotencyKey: fmt.Sprintf("job-%d-%d", time.Now().UnixNano(), i),
			ClientId:       "integration",
			Command:        command,
			CpuMillis:      cpuMillis,
			MemoryBytes:    memBytes,
			MaxAttempts:    5,
		})
		ids = append(ids, res.JobId)
	}
	return ids
}

func (c *cluster) getJob(id string) *pb.Job {
	c.t.Helper()
	res, err := c.client.GetJob(context.Background(), &pb.GetJobRequest{JobId: id, IncludeAttempts: true})
	if err != nil {
		c.t.Fatalf("get job %s: %v", id, err)
	}
	return res.Job
}

// waitForTerminal blocks until every job reaches a terminal state.
func (c *cluster) waitForTerminal(ids []string, timeout time.Duration) map[string]*pb.Job {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	out := make(map[string]*pb.Job, len(ids))

	for time.Now().Before(deadline) {
		pending := 0
		for _, id := range ids {
			if _, done := out[id]; done {
				continue
			}
			j := c.getJob(id)
			if isTerminal(j.State) {
				out[id] = j
				continue
			}
			pending++
		}
		if pending == 0 {
			return out
		}
		time.Sleep(50 * time.Millisecond)
	}

	var stuck []string
	for _, id := range ids {
		if _, done := out[id]; !done {
			j := c.getJob(id)
			stuck = append(stuck, fmt.Sprintf("%s=%s(attempts %d/%d)", id, j.State, j.AttemptCount, j.MaxAttempts))
			if len(stuck) == 5 {
				break
			}
		}
	}
	c.t.Fatalf("%d of %d jobs were still pending after %s; e.g. %v",
		len(ids)-len(out), len(ids), timeout, stuck)
	return nil
}

// checkInvariants runs the full invariant suite and fails the test on any violation.
func (c *cluster) checkInvariants(acceptedJobIDs []string) *invariants.Report {
	c.t.Helper()
	rep, err := invariants.Check(context.Background(), c.store)
	if err != nil {
		c.t.Fatalf("invariant check: %v", err)
	}
	missing, err := invariants.CheckAcceptedJobs(context.Background(), c.store, acceptedJobIDs)
	if err != nil {
		c.t.Fatalf("accepted-job check: %v", err)
	}
	rep.Violations = append(rep.Violations, missing...)

	if !rep.OK() {
		c.t.Errorf("invariant violations:\n%s", rep)
	}
	return rep
}

func isTerminal(s pb.JobState) bool {
	switch s {
	case pb.JobState_JOB_STATE_SUCCEEDED, pb.JobState_JOB_STATE_FAILED, pb.JobState_JOB_STATE_CANCELED:
		return true
	}
	return false
}

// fastConfig shortens every timer so that failure detection and recovery happen on
// a test's timescale instead of a production one. The ratios between the thresholds
// are preserved, because those ratios are what the behaviour depends on.
func fastConfig(policy scheduler.Policy) scheduler.Config {
	return scheduler.Config{
		Policy:            policy,
		Queue:             scheduler.QueueConfig{Ordering: scheduler.OrderPriorityAging, AgingRate: 1, AgingCap: 50},
		LeaseTTL:          2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		SuspectAfter:      600 * time.Millisecond,
		DeadAfter:         1500 * time.Millisecond,
		DispatchInterval:  20 * time.Millisecond,
		ReconcileInterval: 100 * time.Millisecond,
		MaxDispatchBatch:  128,
		RetryBaseDelay:    50 * time.Millisecond,
		RetryMaxDelay:     500 * time.Millisecond,
		Admission: scheduler.AdmissionConfig{
			MaxQueueDepth:        10000,
			MaxInFlightPerClient: 10000,
			RejectUnschedulable:  true,
			DefaultMaxAttempts:   3,
		},
	}
}

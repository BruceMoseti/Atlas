// Package scheduler is Atlas's control plane: it decides where jobs run, hands out
// attempt-scoped leases, detects failures, and retries.
//
// The scheduler owns placement. Workers do not choose their own work; they long-poll
// AcquireJob for assignments the dispatcher has already decided on. That keeps the
// placement policy centralized and comparable (see policies.go) while leaving every
// network connection worker-initiated, so the scheduler never needs to reach a worker.
//
// Two pieces of state exist in parallel and the distinction matters:
//
//   - The SQLite store is authoritative. Nothing is true until it commits.
//   - The in-memory fleet and ready queue are caches that make the dispatch hot loop
//     fast. Every placement is re-validated against the store inside the assignment
//     transaction, so a stale cache can cost a wasted decision but can never
//     oversubscribe a worker or resurrect a terminal job.
package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/BruceMoseti/Atlas/internal/logging"
	"github.com/BruceMoseti/Atlas/internal/metrics"
	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// Errors the API layer maps onto gRPC status codes.
var (
	// ErrNotFound means no such job, attempt, or worker.
	ErrNotFound = errors.New("not found")
	// ErrStaleAttempt means the caller referenced an attempt that no longer holds
	// authority over its job. This is the resurrected-worker case; see
	// docs/SEMANTICS.md §4.
	ErrStaleAttempt = errors.New("stale attempt: lease is no longer valid")
	// ErrUnknownWorker means the worker must register before continuing.
	ErrUnknownWorker = errors.New("unknown worker: registration required")
	// ErrStaleGeneration means the worker restarted and is using credentials from
	// a previous incarnation.
	ErrStaleGeneration = errors.New("stale worker generation: re-registration required")
)

// AdmissionError is returned when admission control refuses a submission. Reason is a
// short machine-readable token, used as the metric label.
type AdmissionError struct {
	Reason  string
	Message string
}

func (e *AdmissionError) Error() string { return e.Reason + ": " + e.Message }

// IdempotencyConflictError means an idempotency key was reused with a materially
// different job spec. Silently returning the old job would be worse: the client would
// believe it had submitted work that Atlas never saw.
type IdempotencyConflictError struct {
	Key           string
	ExistingJobID string
}

func (e *IdempotencyConflictError) Error() string {
	return fmt.Sprintf("idempotency key %q already used by job %s with different parameters", e.Key, e.ExistingJobID)
}

// Config tunes the scheduler. Zero values are replaced by the defaults in
// DefaultConfig during New.
type Config struct {
	// Policy selects a worker for a job. Defaults to best-fit.
	Policy Policy
	// Queue controls the order jobs are considered in.
	Queue QueueConfig

	// LeaseTTL is how long an assignment stays valid without renewal. It bounds
	// how long a silently dead worker can hold capacity hostage.
	LeaseTTL time.Duration
	// HeartbeatInterval is advertised to workers at registration.
	HeartbeatInterval time.Duration
	// SuspectAfter is the heartbeat age at which a worker stops receiving new
	// work. It does not lose its existing assignments.
	SuspectAfter time.Duration
	// DeadAfter is the heartbeat age at which every assignment on the worker is
	// reclaimed. Two thresholds, rather than one, keep a single late heartbeat
	// from costing a round of duplicate executions.
	DeadAfter time.Duration
	// WorkerPurgeAfter is how long a DEAD worker with no live attempts is kept
	// before its row is deleted. Zero disables purging.
	WorkerPurgeAfter time.Duration

	// DispatchInterval bounds how long a dispatchable job waits when no event
	// wakes the dispatcher. Submissions and completions wake it immediately.
	DispatchInterval time.Duration
	// ReconcileInterval is how often leases and worker health are swept.
	ReconcileInterval time.Duration
	// MaxDispatchBatch is how many placements are committed in one transaction.
	// Batching amortizes the fsync that makes each assignment durable.
	MaxDispatchBatch int
	// MaxDispatchScan bounds how far down the queue one sweep looks for
	// something that fits. This is the backfill window: a job too large for the
	// current cluster must not block smaller jobs behind it, but scanning a
	// hundred thousand queued jobs every 25ms to discover that nothing fits is
	// worse than waiting for the next sweep.
	MaxDispatchScan int
	// MaxLeaseSweep bounds how many expired leases one reconcile pass reclaims.
	MaxLeaseSweep int

	// RetryBaseDelay and RetryMaxDelay bound the exponential backoff applied
	// before a retryable failure is requeued.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	Admission AdmissionConfig

	Logger  *slog.Logger
	Metrics *metrics.Metrics
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// DefaultConfig returns the settings Atlas ships with. The lease and heartbeat
// numbers are deliberately short: this is a single-datacentre scheduler and fast
// failure detection is worth more than tolerating a slow network.
func DefaultConfig() Config {
	return Config{
		Queue:             QueueConfig{Ordering: OrderPriorityAging, AgingRate: 1.0, AgingCap: 50},
		LeaseTTL:          15 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		SuspectAfter:      6 * time.Second,
		DeadAfter:         15 * time.Second,
		WorkerPurgeAfter:  1 * time.Hour,
		DispatchInterval:  25 * time.Millisecond,
		ReconcileInterval: 250 * time.Millisecond,
		MaxDispatchBatch:  256,
		MaxDispatchScan:   2048,
		MaxLeaseSweep:     512,
		RetryBaseDelay:    250 * time.Millisecond,
		RetryMaxDelay:     30 * time.Second,
		Admission:         DefaultAdmissionConfig(),
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Policy == nil {
		c.Policy = BestFit{}
	}
	if c.Queue.Ordering == "" {
		c.Queue = d.Queue
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = d.LeaseTTL
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = d.HeartbeatInterval
	}
	if c.SuspectAfter <= 0 {
		c.SuspectAfter = d.SuspectAfter
	}
	if c.DeadAfter <= 0 {
		c.DeadAfter = d.DeadAfter
	}
	if c.DispatchInterval <= 0 {
		c.DispatchInterval = d.DispatchInterval
	}
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = d.ReconcileInterval
	}
	if c.MaxDispatchBatch <= 0 {
		c.MaxDispatchBatch = d.MaxDispatchBatch
	}
	if c.MaxDispatchScan <= 0 {
		c.MaxDispatchScan = d.MaxDispatchScan
	}
	if c.MaxDispatchScan < c.MaxDispatchBatch {
		c.MaxDispatchScan = c.MaxDispatchBatch
	}
	if c.MaxLeaseSweep <= 0 {
		c.MaxLeaseSweep = d.MaxLeaseSweep
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = d.RetryBaseDelay
	}
	if c.RetryMaxDelay <= 0 {
		c.RetryMaxDelay = d.RetryMaxDelay
	}
	if c.Logger == nil {
		c.Logger = logging.Discard()
	}
	if c.Metrics == nil {
		c.Metrics = metrics.NewNop()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	c.Admission.applyDefaults()
}

// Assignment is one unit of work handed to a worker, scoped by a lease.
type Assignment struct {
	JobID          string
	AttemptID      string
	AttemptNumber  int32
	LeaseID        string
	LeaseExpiresAt time.Time
	Image          string
	Command        []string
	Env            map[string]string
	Request        types.Resources
	Timeout        time.Duration
}

// workerSlot is the scheduler's per-worker runtime state: its placement view, its
// health, and its mailbox of undelivered assignments.
type workerSlot struct {
	view *WorkerView

	state         state.WorkerState
	generation    int64
	lastHeartbeat time.Time
	// persistedHeartbeat tracks what is in the database, so routine heartbeats
	// do not cost an fsync each. Heartbeat freshness only has to survive in
	// memory: recovery marks every worker SUSPECT regardless.
	persistedHeartbeat time.Time

	// mailboxMu guards pending. Lock ordering is Scheduler.mu then mailboxMu,
	// never the reverse: nothing acquires Scheduler.mu while holding mailboxMu.
	mailboxMu sync.Mutex
	pending   []Assignment
	signal    chan struct{}
}

func (ws *workerSlot) deliver(a Assignment) {
	ws.mailboxMu.Lock()
	ws.pending = append(ws.pending, a)
	ws.mailboxMu.Unlock()
	select {
	case ws.signal <- struct{}{}:
	default:
	}
}

func (ws *workerSlot) drain() []Assignment {
	ws.mailboxMu.Lock()
	defer ws.mailboxMu.Unlock()
	if len(ws.pending) == 0 {
		return nil
	}
	out := ws.pending
	ws.pending = nil
	return out
}

// Scheduler is the control plane. It is safe for concurrent use.
type Scheduler struct {
	cfg   Config
	store *store.Store
	log   *slog.Logger
	met   *metrics.Metrics
	now   func() time.Time

	mu      sync.Mutex
	queue   *ReadyQueue
	workers map[string]*workerSlot
	// fleet is a stable slice of the same WorkerView pointers held by workers,
	// so placement policies can scan it without allocating per decision.
	fleet []*WorkerView

	wake    chan struct{}
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// New builds a scheduler over an open store. Call Start to run the background loops.
func New(st *store.Store, cfg Config) *Scheduler {
	cfg.applyDefaults()
	return &Scheduler{
		cfg:     cfg,
		store:   st,
		log:     cfg.Logger,
		met:     cfg.Metrics,
		now:     cfg.Now,
		queue:   NewReadyQueue(cfg.Queue),
		workers: make(map[string]*workerSlot),
		wake:    make(chan struct{}, 1),
	}
}

// Config returns the effective configuration.
func (s *Scheduler) Config() Config { return s.cfg }

// Start recovers durable state and launches the dispatch and reconcile loops.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("scheduler: already started")
	}
	s.started = true
	s.mu.Unlock()

	if err := s.Recover(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel

	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.dispatchLoop(runCtx) }()
	go func() { defer s.wg.Done(); s.reconcileLoop(runCtx) }()
	return nil
}

// Stop halts the background loops and waits for them to exit. In-flight jobs keep
// running on their workers; their leases will expire and be reclaimed by whichever
// scheduler process comes back.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// Recover rebuilds every piece of in-memory state from the store. It runs at startup
// and is safe to call on a fresh database.
//
// The three things that must be true afterwards: no worker is believed healthy on the
// strength of a heartbeat we never saw, every worker's allocation matches the attempts
// actually live on it (invariant I3), and no lease that lapsed while the process was
// down is still treated as valid.
func (s *Scheduler) Recover(ctx context.Context) error {
	start := s.now()

	var (
		workers []*types.Worker
		queued  []*types.Job
		running map[string]int
	)

	err := s.store.Update(ctx, func(tx *store.Tx) error {
		all, err := tx.ListWorkers()
		if err != nil {
			return err
		}
		live, err := tx.LiveAttempts("")
		if err != nil {
			return err
		}

		// Recompute allocations from the attempts that are actually live. This
		// re-establishes I3 by construction rather than trusting the
		// denormalized columns a crash may have left behind.
		alloc := make(map[string]types.Resources, len(all))
		for _, a := range live {
			alloc[a.WorkerID] = alloc[a.WorkerID].Add(a.Request)
		}
		for _, w := range all {
			w.Allocated = alloc[w.ID]
			// We have not seen a heartbeat from anyone in this process, and
			// every connection died with the old one. SUSPECT is the honest
			// state; a single heartbeat promotes it back to HEALTHY.
			if w.State == state.WorkerHealthy {
				w.State = state.WorkerSuspect
			}
			if err := tx.SaveWorker(w); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scheduler: recover fleet: %w", err)
	}

	// Leases that lapsed during the outage are reclaimed before anything is
	// dispatched, so the queue we build below already contains their jobs.
	lapsed, err := s.sweepExpiredLeases(ctx)
	if err != nil {
		return fmt.Errorf("scheduler: recover leases: %w", err)
	}

	// Re-read the fleet: the sweep above released capacity for every lapsed
	// lease, so the rows captured before it are stale.
	err = s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if workers, err = tx.ListWorkers(); err != nil {
			return err
		}
		live, err := tx.LiveAttempts("")
		if err != nil {
			return err
		}
		running = make(map[string]int, len(workers))
		for _, a := range live {
			running[a.WorkerID]++
		}
		queued, err = tx.ListJobs(store.JobFilter{States: []state.JobState{state.JobQueued}})
		return err
	})
	if err != nil {
		return fmt.Errorf("scheduler: recover queue: %w", err)
	}

	s.mu.Lock()
	s.workers = make(map[string]*workerSlot, len(workers))
	s.fleet = s.fleet[:0]
	for _, w := range workers {
		s.addWorkerLocked(w, running[w.ID])
	}
	s.queue = NewReadyQueue(s.cfg.Queue)
	restored := make([]*QueuedJob, 0, len(queued))
	for _, j := range queued {
		restored = append(restored, queuedJobFrom(j))
	}
	sortByEnqueuedAt(restored)
	for _, qj := range restored {
		s.queue.Push(qj)
	}
	queueLen := s.queue.Len()
	s.mu.Unlock()

	s.refreshFleetMetrics()
	s.log.Info("scheduler_recovered",
		"workers", len(workers),
		"queued_jobs", queueLen,
		"lapsed_leases", lapsed,
		"duration_ms", s.now().Sub(start).Milliseconds())
	s.signalDispatch()
	return nil
}

func queuedJobFrom(j *types.Job) *QueuedJob {
	qj := &QueuedJob{
		JobID:      j.ID,
		Priority:   j.Priority,
		Request:    j.Request,
		EnqueuedAt: j.EnqueuedAt,
		EligibleAt: j.EligibleAt,
		heapIndex:  -1,
	}
	if j.Deadline != nil {
		qj.Deadline = *j.Deadline
	}
	if qj.EligibleAt.IsZero() {
		qj.EligibleAt = qj.EnqueuedAt
	}
	return qj
}

// addWorkerLocked inserts or refreshes a worker slot and sets its live-attempt count
// to an absolute value. Caller holds s.mu.
func (s *Scheduler) addWorkerLocked(w *types.Worker, running int) *workerSlot {
	ws, ok := s.workers[w.ID]
	if !ok {
		ws = &workerSlot{
			view:   &WorkerView{ID: w.ID},
			signal: make(chan struct{}, 1),
		}
		s.workers[w.ID] = ws
		s.fleet = append(s.fleet, ws.view)
	}
	ws.state = w.State
	ws.generation = w.Generation
	if w.LastHeartbeatAt.After(ws.lastHeartbeat) {
		ws.lastHeartbeat = w.LastHeartbeatAt
		ws.persistedHeartbeat = w.LastHeartbeatAt
	}
	ws.view.Capacity = w.Capacity
	ws.view.Available = w.Capacity.Sub(w.Allocated)
	ws.view.Labels = w.Labels
	ws.view.Schedulable = w.State.Schedulable()
	ws.view.Running = running
	return ws
}

// syncWorkerLocked overwrites the cached view from an authoritative row.
func (s *Scheduler) syncWorkerLocked(w *types.Worker) {
	ws, ok := s.workers[w.ID]
	if !ok {
		s.addWorkerLocked(w, 0)
		return
	}
	ws.state = w.State
	ws.generation = w.Generation
	ws.view.Capacity = w.Capacity
	ws.view.Available = w.Capacity.Sub(w.Allocated)
	ws.view.Schedulable = w.State.Schedulable()
}

func (s *Scheduler) removeWorkerLocked(id string) {
	ws, ok := s.workers[id]
	if !ok {
		return
	}
	delete(s.workers, id)
	for i, v := range s.fleet {
		if v == ws.view {
			s.fleet = append(s.fleet[:i], s.fleet[i+1:]...)
			break
		}
	}
}

// signalDispatch wakes the dispatcher without blocking. The channel has capacity one:
// a pending wakeup already covers any number of new events.
func (s *Scheduler) signalDispatch() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// ClusterStatus is a point-in-time summary for the CLI and the readiness endpoint.
type ClusterStatus struct {
	WorkersTotal     int
	WorkersHealthy   int
	WorkersByState   map[state.WorkerState]int
	Capacity         types.Resources
	Allocated        types.Resources
	CPUUtilization   float64
	MemUtilization   float64
	JobsQueued       int
	JobsQueuedReady  int
	JobsRunning      int
	SchedulingPolicy string
	QueueOrdering    string
}

// Status summarizes the cluster from in-memory state, without touching the database.
func (s *Scheduler) Status() ClusterStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := ClusterStatus{
		WorkersByState:   make(map[state.WorkerState]int),
		SchedulingPolicy: s.cfg.Policy.Name(),
		QueueOrdering:    string(s.cfg.Queue.Ordering),
		JobsQueued:       s.queue.Len(),
		JobsQueuedReady:  s.queue.Ready(),
	}
	for _, ws := range s.workers {
		st.WorkersTotal++
		st.WorkersByState[ws.state]++
		if ws.state == state.WorkerHealthy {
			st.WorkersHealthy++
		}
		// Capacity on DEAD workers is not schedulable and must not inflate
		// utilization denominators.
		if ws.state == state.WorkerDead {
			continue
		}
		st.Capacity = st.Capacity.Add(ws.view.Capacity)
		st.Allocated = st.Allocated.Add(ws.view.Capacity.Sub(ws.view.Available))
		st.JobsRunning += ws.view.Running
	}
	st.CPUUtilization, st.MemUtilization = Utilization(st.Allocated, st.Capacity)
	return st
}

// refreshFleetMetrics publishes the gauges that describe cluster occupancy.
func (s *Scheduler) refreshFleetMetrics() {
	st := s.Status()
	s.met.WorkersTotal.Set(float64(st.WorkersTotal))
	for _, ws := range []state.WorkerState{state.WorkerHealthy, state.WorkerSuspect, state.WorkerDead, state.WorkerDraining, state.WorkerDrained} {
		s.met.WorkersByState.WithLabelValues(string(ws)).Set(float64(st.WorkersByState[ws]))
	}
	s.met.WorkerCPUCapacity.Set(float64(st.Capacity.CPUMillis))
	s.met.WorkerMemCapacity.Set(float64(st.Capacity.MemoryBytes))
	s.met.WorkerCPUAllocated.Set(float64(st.Allocated.CPUMillis))
	s.met.WorkerMemAllocated.Set(float64(st.Allocated.MemoryBytes))
	s.met.CPUUtilization.Set(st.CPUUtilization)
	s.met.MemUtilization.Set(st.MemUtilization)
	s.met.QueueDepth.Set(float64(st.JobsQueued))
	s.met.JobsQueued.WithLabelValues("true").Set(float64(st.JobsQueuedReady))
	s.met.JobsQueued.WithLabelValues("false").Set(float64(st.JobsQueued - st.JobsQueuedReady))
	s.met.JobsRunning.Set(float64(st.JobsRunning))
}

// newID returns a short, prefixed, globally unique identifier. Prefixes make logs
// readable: you can tell a lease from an attempt at a glance.
func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means the process is in no state to schedule
		// anything; a unique-enough fallback keeps the error path from
		// spreading through every call site.
		return prefix + "_" + hex.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))[:16]
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

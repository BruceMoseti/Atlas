package simulator

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// Config describes one simulation run.
type Config struct {
	Fleet      []WorkerClass
	JobClasses []JobClass
	Jobs       int

	// ArrivalRate is jobs per second of virtual time. Zero means every job is
	// present at t=0, which measures pure packing quality without the arrival
	// process mixed in.
	ArrivalRate float64

	Policy   scheduler.Policy
	Ordering scheduler.Ordering
	// AgingRate and AgingCap only matter for the priority-aging ordering.
	AgingRate float64
	AgingCap  float64

	// MaxQueueDepth applies admission control. Zero means an unbounded queue,
	// which is the control case for the overload experiment.
	MaxQueueDepth int

	// WorkerFailureRate is the expected number of failures per worker per
	// simulated hour. A failed worker loses everything it was running.
	WorkerFailureRate float64
	// WorkerRecovery is how long a failed worker stays down.
	WorkerRecovery time.Duration
	// LeaseTTL models detection latency: work lost on a failed worker is not
	// requeued until its lease would have expired.
	LeaseTTL time.Duration
	// MaxAttempts bounds retries, as in the real scheduler.
	MaxAttempts int
	// MaxDispatchScan is the backfill window, matching the real scheduler's
	// setting of the same name. It is configurable here because how far the
	// dispatcher looks past an unplaceable job directly changes how much
	// capacity ends up stranded.
	MaxDispatchScan int

	Seed int64
	// StopAfter abandons a run that has not drained, so a pathological
	// configuration cannot hang a benchmark.
	StopAfter time.Duration
}

func (c *Config) applyDefaults() {
	if len(c.Fleet) == 0 {
		c.Fleet = UniformFleet(100)
	}
	if len(c.JobClasses) == 0 {
		c.JobClasses = MixedWorkload()
	}
	if c.Jobs <= 0 {
		c.Jobs = 10000
	}
	if c.Policy == nil {
		c.Policy = scheduler.BestFit{}
	}
	if c.Ordering == "" {
		c.Ordering = scheduler.OrderFIFO
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 15 * time.Second
	}
	if c.WorkerRecovery <= 0 {
		c.WorkerRecovery = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.MaxDispatchScan <= 0 {
		c.MaxDispatchScan = scheduler.DefaultConfig().MaxDispatchScan
	}
	if c.StopAfter <= 0 {
		c.StopAfter = 365 * 24 * time.Hour
	}
}

// Result is what one run measured.
type Result struct {
	Policy        string
	Ordering      string
	WorkersTotal  int
	JobsSubmitted int
	JobsAccepted  int
	JobsRejected  int
	JobsCompleted int
	JobsAbandoned int

	// Wait is seconds of virtual time between a job arriving and first being
	// placed on a worker.
	WaitMean float64
	WaitP50  float64
	WaitP95  float64
	WaitP99  float64
	WaitMax  float64

	// Turnaround is arrival to final completion, so it includes every retry.
	TurnaroundMean float64
	TurnaroundP99  float64

	// Makespan is virtual seconds from the first arrival to the last completion.
	Makespan float64

	// Utilization is time-weighted mean allocation over the makespan, counting
	// only the time the cluster had work to do.
	CPUUtilization float64
	MemUtilization float64
	// StrandedCPU and StrandedMemory are the mean fraction of the fleet that
	// was free while at least one job was waiting for capacity, averaged over
	// exactly that time. This is fragmentation made measurable: capacity that
	// existed, was idle, and still could not be used.
	//
	// They are normalized against time-under-demand rather than the whole run,
	// because free capacity during the drain tail is not fragmentation, it is
	// just an empty cluster.
	StrandedCPU    float64
	StrandedMemory float64
	// DemandFraction is how much of the run had a non-empty queue, so a reader
	// can tell whether the stranded numbers cover most of the run or a sliver
	// of it.
	DemandFraction float64

	Attempts         int
	Retries          int
	LostAttempts     int
	WorkerFailures   int
	PlacementSweeps  int
	PlacementSkipped int

	// RealDecisionTime is wall-clock time spent inside Policy.Select, and
	// DecisionsPerSecond derives from it. This is the only number here that is
	// about the implementation rather than the model.
	RealDecisionTime   time.Duration
	Decisions          int
	DecisionsPerSecond float64

	// PeakQueueDepth is the largest number of jobs waiting at once.
	PeakQueueDepth int

	// ByClass breaks the wait distribution down per job class. Aggregate
	// numbers hide starvation completely: a scheduler that serves 70% of its
	// work instantly and never runs the other 30% has a respectable mean.
	ByClass map[string]ClassStats
}

// ClassStats is the wait distribution for one job class.
type ClassStats struct {
	Name      string
	Count     int
	Completed int
	WaitMean  float64
	WaitP50   float64
	WaitP99   float64
	WaitMax   float64
}

// Run executes the simulation to completion and returns its measurements.
func Run(cfg Config) Result {
	cfg.applyDefaults()
	s := newSim(cfg)
	s.run()
	return s.result()
}

// ---------------------------------------------------------------- events

type eventKind int

const (
	evArrival eventKind = iota
	evCompletion
	evWorkerFail
	evWorkerRecover
	evRequeue
	evDispatch
)

type event struct {
	at   float64 // virtual seconds
	kind eventKind
	seq  int64
	job  *simJob
	wrk  *simWorker
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	// Stable tie-break so a run is reproducible for a given seed.
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any     { old := *q; n := len(old); e := old[n-1]; *q = old[:n-1]; return e }

// ---------------------------------------------------------------- state

type simJob struct {
	id       string
	class    string
	request  types.Resources
	priority int32
	runtime  float64

	arrivedAt float64
	// firstRunAt and finishedAt are negative until they happen, because zero is
	// a legitimate instant in a run where every job arrives at t=0.
	firstRunAt float64
	finishedAt float64

	attempts int
	worker   *simWorker
	done     bool
	placed   bool
}

type simWorker struct {
	view    *scheduler.WorkerView
	running map[string]*simJob
	up      bool
}

type sim struct {
	cfg   Config
	rng   *rand.Rand
	now   float64
	seq   int64
	queue *scheduler.ReadyQueue

	events  eventQueue
	workers []*simWorker
	fleet   []*scheduler.WorkerView
	byID    map[string]*simWorker
	jobs    map[string]*simJob

	capacity types.Resources

	// Time-weighted integrals, accumulated on every allocation change.
	lastAccrual   float64
	allocCPUArea  float64
	allocMemArea  float64
	strandCPUArea float64
	strandMemArea float64
	busyTime      float64
	demandTime    float64

	res          Result
	dispatchDue  bool
	firstArrival float64
	lastFinish   float64
}

func newSim(cfg Config) *sim {
	s := &sim{
		cfg:  cfg,
		rng:  rand.New(rand.NewSource(cfg.Seed)),
		jobs: make(map[string]*simJob, cfg.Jobs),
		byID: make(map[string]*simWorker),
		queue: scheduler.NewReadyQueue(scheduler.QueueConfig{
			Ordering:  cfg.Ordering,
			AgingRate: cfg.AgingRate,
			AgingCap:  cfg.AgingCap,
		}),
		firstArrival: -1,
	}

	n := 0
	for _, wc := range cfg.Fleet {
		for i := 0; i < wc.Count; i++ {
			id := fmt.Sprintf("%s-%d", wc.Name, i)
			cap := types.Resources{CPUMillis: wc.CPUMillis, MemoryBytes: wc.MemoryBytes}
			view := &scheduler.WorkerView{ID: id, Capacity: cap, Available: cap, Schedulable: true}
			w := &simWorker{view: view, running: map[string]*simJob{}, up: true}
			s.workers = append(s.workers, w)
			s.fleet = append(s.fleet, view)
			s.byID[id] = w
			s.capacity = s.capacity.Add(cap)
			n++
		}
	}
	s.res.WorkersTotal = n
	s.res.Policy = cfg.Policy.Name()
	s.res.Ordering = string(cfg.Ordering)

	s.scheduleArrivals()
	s.scheduleFailures()
	return s
}

// scheduleArrivals lays out the arrival process up front. Doing it eagerly keeps
// the event loop simple and makes a run exactly reproducible from its seed.
func (s *sim) scheduleArrivals() {
	t := 0.0
	for i := 0; i < s.cfg.Jobs; i++ {
		if s.cfg.ArrivalRate > 0 {
			t += s.rng.ExpFloat64() / s.cfg.ArrivalRate
		}
		class := pickClass(s.rng, s.cfg.JobClasses)
		j := &simJob{
			id:         fmt.Sprintf("job-%d", i),
			class:      class.Name,
			request:    types.Resources{CPUMillis: class.CPUMillis, MemoryBytes: class.MemoryBytes},
			priority:   class.Priority,
			runtime:    class.Runtime.Sample(s.rng),
			arrivedAt:  t,
			firstRunAt: -1,
			finishedAt: -1,
		}
		if j.runtime < 0.001 {
			j.runtime = 0.001
		}
		s.jobs[j.id] = j
		s.push(&event{at: t, kind: evArrival, job: j})
	}
}

// scheduleFailures draws each worker's failure times from a Poisson process.
func (s *sim) scheduleFailures() {
	if s.cfg.WorkerFailureRate <= 0 {
		return
	}
	perSecond := s.cfg.WorkerFailureRate / 3600.0
	horizon := seconds(s.cfg.StopAfter)
	if horizon > 24*3600 {
		horizon = 24 * 3600
	}
	for _, w := range s.workers {
		t := 0.0
		for {
			t += s.rng.ExpFloat64() / perSecond
			if t > horizon {
				break
			}
			s.push(&event{at: t, kind: evWorkerFail, wrk: w})
		}
	}
}

func (s *sim) push(e *event) {
	s.seq++
	e.seq = s.seq
	heap.Push(&s.events, e)
}

// requestDispatch queues a placement sweep at the current instant, collapsing
// repeated requests so one sweep handles everything that just changed.
func (s *sim) requestDispatch() {
	if s.dispatchDue {
		return
	}
	s.dispatchDue = true
	s.push(&event{at: s.now, kind: evDispatch})
}

func (s *sim) run() {
	limit := seconds(s.cfg.StopAfter)
	for s.events.Len() > 0 {
		e := heap.Pop(&s.events).(*event)
		if e.at > limit {
			break
		}
		s.accrue(e.at)
		s.now = e.at

		switch e.kind {
		case evArrival:
			s.onArrival(e.job)
		case evCompletion:
			s.onCompletion(e.job)
		case evWorkerFail:
			s.onWorkerFail(e.wrk)
		case evWorkerRecover:
			s.onWorkerRecover(e.wrk)
		case evRequeue:
			s.onRequeue(e.job)
		case evDispatch:
			s.dispatchDue = false
			s.dispatch()
		}
	}
	s.accrue(s.now)
}

// accrue advances the time-weighted utilization and fragmentation integrals to t.
func (s *sim) accrue(t float64) {
	dt := t - s.lastAccrual
	s.lastAccrual = t
	if dt <= 0 {
		return
	}

	var allocated types.Resources
	var schedulableCap types.Resources
	for _, w := range s.workers {
		if !w.up {
			continue
		}
		schedulableCap = schedulableCap.Add(w.view.Capacity)
		allocated = allocated.Add(w.view.Capacity.Sub(w.view.Available))
	}

	// Only count time when the cluster had something to do. Idle time at the
	// tail of a run would otherwise drag every utilization number toward zero
	// and make the policies look identical.
	queued := s.queue.Len()
	if allocated.CPUMillis == 0 && queued == 0 {
		return
	}
	s.busyTime += dt

	if schedulableCap.CPUMillis > 0 {
		s.allocCPUArea += dt * float64(allocated.CPUMillis) / float64(schedulableCap.CPUMillis)
	}
	if schedulableCap.MemoryBytes > 0 {
		s.allocMemArea += dt * float64(allocated.MemoryBytes) / float64(schedulableCap.MemoryBytes)
	}

	// Stranded capacity: free resources while jobs are waiting. That is
	// fragmentation, as opposed to simply having nothing to run.
	if queued > 0 {
		s.demandTime += dt
		if schedulableCap.CPUMillis > 0 {
			free := schedulableCap.CPUMillis - allocated.CPUMillis
			s.strandCPUArea += dt * float64(free) / float64(schedulableCap.CPUMillis)
		}
		if schedulableCap.MemoryBytes > 0 {
			free := schedulableCap.MemoryBytes - allocated.MemoryBytes
			s.strandMemArea += dt * float64(free) / float64(schedulableCap.MemoryBytes)
		}
	}
}

func (s *sim) onArrival(j *simJob) {
	if s.firstArrival < 0 {
		s.firstArrival = j.arrivedAt
	}
	s.res.JobsSubmitted++

	if s.cfg.MaxQueueDepth > 0 && s.queue.Len() >= s.cfg.MaxQueueDepth {
		s.res.JobsRejected++
		j.done = true
		return
	}
	s.res.JobsAccepted++
	s.enqueue(j, j.arrivedAt)
}

func (s *sim) enqueue(j *simJob, at float64) {
	s.queue.Push(&scheduler.QueuedJob{
		JobID:      j.id,
		Priority:   j.priority,
		Request:    j.request,
		EnqueuedAt: virtualTime(at),
		EligibleAt: virtualTime(at),
	})
	if d := s.queue.Len(); d > s.res.PeakQueueDepth {
		s.res.PeakQueueDepth = d
	}
	s.requestDispatch()
}

func (s *sim) onCompletion(j *simJob) {
	w := j.worker
	if w == nil || !w.up {
		// The worker failed while this job was on it; the failure path already
		// accounted for the job.
		return
	}
	delete(w.running, j.id)
	w.view.Available = w.view.Available.Add(j.request)
	w.view.Running--

	j.done = true
	j.finishedAt = s.now
	j.worker = nil
	s.res.JobsCompleted++
	if s.now > s.lastFinish {
		s.lastFinish = s.now
	}
	s.requestDispatch()
}

func (s *sim) onWorkerFail(w *simWorker) {
	if !w.up {
		return
	}
	w.up = false
	w.view.Schedulable = false
	s.res.WorkerFailures++

	// Everything on the worker is lost. It is not requeued until the lease
	// would have expired, which is what makes detection latency visible in the
	// wait-time numbers rather than hidden.
	requeueAt := s.now + seconds(s.cfg.LeaseTTL)
	for id, j := range w.running {
		delete(w.running, id)
		w.view.Available = w.view.Available.Add(j.request)
		j.worker = nil
		j.placed = false
		s.res.LostAttempts++
		s.push(&event{at: requeueAt, kind: evRequeue, job: j})
	}
	w.view.Running = 0
	s.push(&event{at: s.now + seconds(s.cfg.WorkerRecovery), kind: evWorkerRecover, wrk: w})
}

func (s *sim) onWorkerRecover(w *simWorker) {
	w.up = true
	w.view.Schedulable = true
	w.view.Available = w.view.Capacity
	s.requestDispatch()
}

func (s *sim) onRequeue(j *simJob) {
	if j.done {
		return
	}
	if j.attempts >= s.cfg.MaxAttempts {
		j.done = true
		s.res.JobsAbandoned++
		return
	}
	s.res.Retries++
	s.enqueue(j, s.now)
}

// dispatch is the simulator's placement sweep. It is deliberately the same shape as
// the real dispatcher: take the best candidate, ask the policy, and move on to the
// next job if the cluster cannot take this one.
func (s *sim) dispatch() {
	s.res.PlacementSweeps++
	var unplaced []*scheduler.QueuedJob
	vnow := virtualTime(s.now)

	for scanned := 0; scanned < s.cfg.MaxDispatchScan; scanned++ {
		qj := s.queue.PopBest(vnow)
		if qj == nil {
			break
		}
		start := time.Now()
		idx, ok := s.cfg.Policy.Select(qj.Request, s.fleet)
		s.res.RealDecisionTime += time.Since(start)
		s.res.Decisions++

		if !ok {
			s.res.PlacementSkipped++
			unplaced = append(unplaced, qj)
			continue
		}

		w := s.byID[s.fleet[idx].ID]
		j := s.jobs[qj.JobID]
		j.attempts++
		j.placed = true
		j.worker = w
		if j.firstRunAt < 0 {
			j.firstRunAt = s.now
		}
		s.res.Attempts++

		w.running[j.id] = j
		w.view.Available = w.view.Available.Sub(j.request)
		w.view.Running++
		s.push(&event{at: s.now + j.runtime, kind: evCompletion, job: j})
	}
	s.queue.Requeue(unplaced)
}

func (s *sim) result() Result {
	r := s.res

	waits := make([]float64, 0, len(s.jobs))
	turnarounds := make([]float64, 0, len(s.jobs))
	perClass := make(map[string][]float64)
	classCount := make(map[string]int)
	classDone := make(map[string]int)

	for _, j := range s.jobs {
		classCount[j.class]++
		if j.firstRunAt >= 0 {
			wait := j.firstRunAt - j.arrivedAt
			waits = append(waits, wait)
			perClass[j.class] = append(perClass[j.class], wait)
		}
		if j.finishedAt >= 0 {
			turnarounds = append(turnarounds, j.finishedAt-j.arrivedAt)
			classDone[j.class]++
		}
	}
	sort.Float64s(waits)
	sort.Float64s(turnarounds)

	r.ByClass = make(map[string]ClassStats, len(perClass))
	for name, total := range classCount {
		ws := perClass[name]
		sort.Float64s(ws)
		cs := ClassStats{
			Name:      name,
			Count:     total,
			Completed: classDone[name],
			WaitMean:  mean(ws),
			WaitP50:   percentile(ws, 50),
			WaitP99:   percentile(ws, 99),
		}
		if len(ws) > 0 {
			cs.WaitMax = ws[len(ws)-1]
		}
		r.ByClass[name] = cs
	}

	r.WaitMean = mean(waits)
	r.WaitP50 = percentile(waits, 50)
	r.WaitP95 = percentile(waits, 95)
	r.WaitP99 = percentile(waits, 99)
	if len(waits) > 0 {
		r.WaitMax = waits[len(waits)-1]
	}
	r.TurnaroundMean = mean(turnarounds)
	r.TurnaroundP99 = percentile(turnarounds, 99)

	if s.firstArrival >= 0 && s.lastFinish > s.firstArrival {
		r.Makespan = s.lastFinish - s.firstArrival
	}
	if s.busyTime > 0 {
		r.CPUUtilization = s.allocCPUArea / s.busyTime
		r.MemUtilization = s.allocMemArea / s.busyTime
		r.DemandFraction = s.demandTime / s.busyTime
	}
	if s.demandTime > 0 {
		r.StrandedCPU = s.strandCPUArea / s.demandTime
		r.StrandedMemory = s.strandMemArea / s.demandTime
	}
	if r.RealDecisionTime > 0 {
		r.DecisionsPerSecond = float64(r.Decisions) / r.RealDecisionTime.Seconds()
	}
	return r
}

// virtualTime maps simulated seconds onto a time.Time, because the real ready queue
// works in wall-clock terms. The epoch is arbitrary; only differences matter.
var simEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func virtualTime(sec float64) time.Time {
	return simEpoch.Add(time.Duration(sec * float64(time.Second)))
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// percentile returns the p-th percentile of a sorted slice using nearest-rank.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p / 100)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

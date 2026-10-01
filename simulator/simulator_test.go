package simulator

import (
	"container/heap"
	"math"
	"testing"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// A simulator with a bug produces convincing wrong numbers, which is worse than
// producing none. These tests check the properties the model must have for its
// output in docs/RESULTS.md to mean anything.

func smallConfig() Config {
	return Config{
		Fleet:      UniformFleet(10),
		JobClasses: MixedWorkload(),
		Jobs:       2000,
		Policy:     scheduler.BestFit{},
		Ordering:   scheduler.OrderFIFO,
		Seed:       7,
	}
}

// TestRunsAreDeterministic is what makes a seed worth publishing: a reader who
// reruns an experiment must get the same numbers.
func TestRunsAreDeterministic(t *testing.T) {
	a := Run(smallConfig())
	b := Run(smallConfig())

	if a.JobsCompleted != b.JobsCompleted || a.Attempts != b.Attempts {
		t.Fatalf("job counts differ between identical runs: %d/%d vs %d/%d",
			a.JobsCompleted, a.Attempts, b.JobsCompleted, b.Attempts)
	}
	for _, c := range []struct {
		name string
		x, y float64
	}{
		{"mean wait", a.WaitMean, b.WaitMean},
		{"p99 wait", a.WaitP99, b.WaitP99},
		{"makespan", a.Makespan, b.Makespan},
		{"cpu utilization", a.CPUUtilization, b.CPUUtilization},
		{"stranded cpu", a.StrandedCPU, b.StrandedCPU},
	} {
		if c.x != c.y {
			t.Errorf("%s differs between identical runs: %v vs %v", c.name, c.x, c.y)
		}
	}
}

func TestDifferentSeedsProduceDifferentRuns(t *testing.T) {
	cfg := smallConfig()
	a := Run(cfg)
	cfg.Seed = 99
	b := Run(cfg)
	if a.WaitMean == b.WaitMean && a.Makespan == b.Makespan {
		t.Fatal("two different seeds produced identical results; the seed is not reaching the generator")
	}
}

// TestEveryJobIsAccountedFor is the simulator's version of invariant I6.
func TestEveryJobIsAccountedFor(t *testing.T) {
	cfg := smallConfig()
	cfg.WorkerFailureRate = 30
	cfg.MaxAttempts = 3
	r := Run(cfg)

	if r.JobsSubmitted != cfg.Jobs {
		t.Fatalf("submitted %d jobs, configured %d", r.JobsSubmitted, cfg.Jobs)
	}
	if r.JobsAccepted+r.JobsRejected != r.JobsSubmitted {
		t.Fatalf("accepted %d + rejected %d != submitted %d",
			r.JobsAccepted, r.JobsRejected, r.JobsSubmitted)
	}
	if r.JobsCompleted+r.JobsAbandoned > r.JobsAccepted {
		t.Fatalf("completed %d + abandoned %d exceeds accepted %d",
			r.JobsCompleted, r.JobsAbandoned, r.JobsAccepted)
	}
	if r.JobsCompleted == 0 {
		t.Fatal("no jobs completed at all")
	}
}

// TestNoWorkerIsEverOversubscribed is invariant I2 inside the model. The real
// scheduler has the store refusing it; the simulator has nothing but this check.
func TestNoWorkerIsEverOversubscribed(t *testing.T) {
	cfg := smallConfig()
	cfg.Jobs = 5000
	cfg.WorkerFailureRate = 20

	s := newSim(cfg)
	s.cfg.applyDefaults()

	// Step the event loop by hand so the fleet can be inspected after every
	// event rather than only at the end.
	limit := seconds(s.cfg.StopAfter)
	checks := 0
	for s.events.Len() > 0 {
		e := popEvent(s)
		if e.at > limit {
			break
		}
		s.accrue(e.at)
		s.now = e.at
		dispatchEvent(s, e)

		for _, w := range s.workers {
			if w.view.Available.CPUMillis < 0 || w.view.Available.MemoryBytes < 0 {
				t.Fatalf("worker %s oversubscribed at t=%.3f: available cpu=%d mem=%d",
					w.view.ID, s.now, w.view.Available.CPUMillis, w.view.Available.MemoryBytes)
			}
			if w.view.Available.CPUMillis > w.view.Capacity.CPUMillis {
				t.Fatalf("worker %s has more available cpu than capacity at t=%.3f", w.view.ID, s.now)
			}
		}
		checks++
	}
	if checks < 1000 {
		t.Fatalf("only %d events were processed; the test did not exercise much", checks)
	}
}

// TestUtilizationStaysInRange guards the metric most likely to be silently wrong,
// since it is an integral accumulated across every event.
func TestUtilizationStaysInRange(t *testing.T) {
	r := Run(smallConfig())
	for _, c := range []struct {
		name string
		v    float64
	}{
		{"cpu utilization", r.CPUUtilization},
		{"memory utilization", r.MemUtilization},
		{"stranded cpu", r.StrandedCPU},
		{"stranded memory", r.StrandedMemory},
		{"demand fraction", r.DemandFraction},
	} {
		if c.v < 0 || c.v > 1 || math.IsNaN(c.v) {
			t.Errorf("%s = %v, want a fraction in [0,1]", c.name, c.v)
		}
	}
}

// TestHigherLoadProducesLongerWaits is a sanity check on the arrival process: if
// this did not hold, none of the load-swept experiments would mean anything.
func TestHigherLoadProducesLongerWaits(t *testing.T) {
	fleet := UniformFleet(20)
	classes := MixedWorkload()
	rate := SustainableRate(fleet, classes)

	run := func(load float64) Result {
		return Run(Config{
			Fleet: fleet, JobClasses: classes, Jobs: 5000,
			ArrivalRate: rate * load,
			Policy:      scheduler.LeastLoaded{}, Ordering: scheduler.OrderFIFO, Seed: 3,
		})
	}
	light, heavy := run(0.5), run(1.1)
	if heavy.WaitP99 <= light.WaitP99 {
		t.Fatalf("p99 wait at 110%% load (%.2fs) is not above 50%% load (%.2fs)",
			heavy.WaitP99, light.WaitP99)
	}
}

// TestWorkerFailuresCauseRetries checks that the failure path is wired up at all;
// experiment 2b would otherwise be reporting zeros convincingly.
func TestWorkerFailuresCauseRetries(t *testing.T) {
	cfg := smallConfig()
	cfg.ArrivalRate = 20
	cfg.WorkerFailureRate = 60
	cfg.MaxAttempts = 5
	r := Run(cfg)

	if r.WorkerFailures == 0 {
		t.Fatal("no worker failures were injected")
	}
	if r.LostAttempts == 0 {
		t.Fatal("worker failures were injected but no attempts were lost")
	}
	if r.Retries == 0 {
		t.Fatal("attempts were lost but nothing was retried")
	}
	if r.Attempts <= r.JobsCompleted {
		t.Fatalf("attempts (%d) should exceed completed jobs (%d) once retries happen",
			r.Attempts, r.JobsCompleted)
	}
}

func TestAdmissionControlBoundsTheQueue(t *testing.T) {
	cfg := smallConfig()
	cfg.ArrivalRate = 500 // far beyond capacity
	cfg.MaxQueueDepth = 50
	r := Run(cfg)

	if r.JobsRejected == 0 {
		t.Fatal("a 500/sec arrival rate against a bounded queue rejected nothing")
	}
	if r.PeakQueueDepth > cfg.MaxQueueDepth {
		t.Fatalf("peak queue depth %d exceeded the limit of %d", r.PeakQueueDepth, cfg.MaxQueueDepth)
	}
}

// TestSustainableRateMatchesAHandCalculation pins the arrival-rate estimate that
// every load-swept experiment is normalized against.
func TestSustainableRateMatchesAHandCalculation(t *testing.T) {
	// One worker with 10 cores. One job class needing 1 core for 10 seconds.
	// Ten concurrent jobs fit, each occupying a slot for 10s, so the cluster
	// can retain exactly one job per second.
	fleet := []WorkerClass{{Name: "w", Count: 1, CPUMillis: 10000, MemoryBytes: 100 * gb}}
	classes := []JobClass{{Name: "j", Weight: 1, CPUMillis: 1000, MemoryBytes: 1 * gb, Runtime: Fixed(10)}}

	if got := SustainableRate(fleet, classes); math.Abs(got-1.0) > 1e-9 {
		t.Fatalf("SustainableRate = %v, want 1.0", got)
	}
}

func TestDistributionMeans(t *testing.T) {
	cases := []struct {
		d    Dist
		want float64
	}{
		{Fixed(5), 5},
		{Uniform{Lo: 2, Hi: 8}, 5},
		{Exponential{Mean: 7}, 7},
		{Bimodal{ShortMean: 10, LongMean: 100, LongWeight: 0.1}, 19},
	}
	for _, c := range cases {
		if got := meanOf(c.d); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("meanOf(%s) = %v, want %v", c.d, got, c.want)
		}
	}
}

func TestResourcesAreCopiedIntoTheFleet(t *testing.T) {
	// A shared Resources value aliased across workers would make the whole
	// resource model nonsense, and the symptom would be subtle.
	s := newSim(Config{
		Fleet:      []WorkerClass{{Name: "w", Count: 3, CPUMillis: 1000, MemoryBytes: gb}},
		JobClasses: MixedWorkload(), Jobs: 1,
		Policy: scheduler.BestFit{}, Ordering: scheduler.OrderFIFO,
	})
	s.workers[0].view.Available = types.Resources{}
	for _, w := range s.workers[1:] {
		if w.view.Available.CPUMillis != 1000 {
			t.Fatal("worker views share state")
		}
	}
}

// popEvent and dispatchEvent expose one step of the event loop for the
// oversubscription test. Keeping them in the test file rather than exporting them
// keeps the package's surface honest.
func popEvent(s *sim) *event {
	return heap.Pop(&s.events).(*event)
}

func dispatchEvent(s *sim, e *event) {
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

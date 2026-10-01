package scheduler

import (
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/types"
)

func worker(id string, capCPU, capMem int64, usedCPU, usedMem int64) *WorkerView {
	return &WorkerView{
		ID:          id,
		Capacity:    types.Resources{CPUMillis: capCPU, MemoryBytes: capMem},
		Available:   types.Resources{CPUMillis: capCPU - usedCPU, MemoryBytes: capMem - usedMem},
		Schedulable: true,
	}
}

const gb = int64(1) << 30

func TestFitRequiresBothDimensions(t *testing.T) {
	// A worker with plenty of memory but not enough CPU is not a candidate, and
	// vice versa. Getting this wrong is how schedulers oversubscribe one
	// dimension while reporting the other looks fine.
	w := worker("w", 2000, 16*gb, 0, 0)
	cases := []struct {
		name string
		req  types.Resources
		want bool
	}{
		{"fits exactly", types.Resources{CPUMillis: 2000, MemoryBytes: 16 * gb}, true},
		{"cpu too large", types.Resources{CPUMillis: 4000, MemoryBytes: 8 * gb}, false},
		{"memory too large", types.Resources{CPUMillis: 1000, MemoryBytes: 32 * gb}, false},
		{"both fit", types.Resources{CPUMillis: 500, MemoryBytes: gb}, true},
	}
	for _, c := range cases {
		if got := w.Fits(c.req); got != c.want {
			t.Errorf("%s: Fits = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestUnschedulableWorkersNeverFit(t *testing.T) {
	w := worker("w", 8000, 32*gb, 0, 0)
	w.Schedulable = false
	if w.Fits(types.Resources{CPUMillis: 1, MemoryBytes: 1}) {
		t.Error("a non-schedulable worker must never be a placement candidate, however idle it is")
	}
}

func TestRoundRobinRotates(t *testing.T) {
	fleet := []*WorkerView{
		worker("a", 8000, 32*gb, 0, 0),
		worker("b", 8000, 32*gb, 0, 0),
		worker("c", 8000, 32*gb, 0, 0),
	}
	p := &RoundRobin{}
	req := types.Resources{CPUMillis: 100, MemoryBytes: gb}

	var got []string
	for i := 0; i < 5; i++ {
		idx, ok := p.Select(req, fleet)
		if !ok {
			t.Fatal("round robin found no worker on an idle fleet")
		}
		got = append(got, fleet[idx].ID)
	}
	want := []string{"a", "b", "c", "a", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rotation = %v, want %v", got, want)
		}
	}
}

func TestRoundRobinSkipsWorkersThatDoNotFit(t *testing.T) {
	fleet := []*WorkerView{
		worker("small", 1000, gb, 0, 0),
		worker("large", 16000, 64*gb, 0, 0),
	}
	p := &RoundRobin{}
	req := types.Resources{CPUMillis: 8000, MemoryBytes: 32 * gb}
	for i := 0; i < 3; i++ {
		idx, ok := p.Select(req, fleet)
		if !ok || fleet[idx].ID != "large" {
			t.Fatalf("round robin must skip workers that cannot hold the job")
		}
	}
}

func TestLeastLoadedPicksTheEmptiestWorker(t *testing.T) {
	fleet := []*WorkerView{
		worker("busy", 8000, 32*gb, 6000, 24*gb),
		worker("idle", 8000, 32*gb, 0, 0),
		worker("half", 8000, 32*gb, 4000, 16*gb),
	}
	idx, ok := LeastLoaded{}.Select(types.Resources{CPUMillis: 1000, MemoryBytes: gb}, fleet)
	if !ok || fleet[idx].ID != "idle" {
		t.Fatalf("least-loaded chose %v, want idle", fleetID(fleet, idx, ok))
	}
}

// TestBestFitPacksTightly is the behavioural difference from least-loaded: best-fit
// deliberately fills a partly used worker so that the empty one stays available for
// a job that needs the whole machine.
func TestBestFitPacksTightly(t *testing.T) {
	fleet := []*WorkerView{
		worker("idle", 8000, 32*gb, 0, 0),
		worker("snug", 8000, 32*gb, 6000, 24*gb),
	}
	req := types.Resources{CPUMillis: 2000, MemoryBytes: 8 * gb}

	bfIdx, ok := BestFit{}.Select(req, fleet)
	if !ok || fleet[bfIdx].ID != "snug" {
		t.Fatalf("best-fit chose %v, want snug", fleetID(fleet, bfIdx, ok))
	}
	llIdx, ok := LeastLoaded{}.Select(req, fleet)
	if !ok || fleet[llIdx].ID != "idle" {
		t.Fatalf("least-loaded chose %v, want idle", fleetID(fleet, llIdx, ok))
	}
}

func TestPoliciesReportFailureOnAFullCluster(t *testing.T) {
	fleet := []*WorkerView{worker("w", 1000, gb, 1000, gb)}
	req := types.Resources{CPUMillis: 100, MemoryBytes: 1 << 20}
	for _, p := range []Policy{&RoundRobin{}, LeastLoaded{}, BestFit{}} {
		if _, ok := p.Select(req, fleet); ok {
			t.Errorf("%s claimed to place a job on a full cluster", p.Name())
		}
	}
}

func TestPoliciesHandleAnEmptyFleet(t *testing.T) {
	req := types.Resources{CPUMillis: 100, MemoryBytes: 1 << 20}
	for _, p := range []Policy{&RoundRobin{}, LeastLoaded{}, BestFit{}} {
		if _, ok := p.Select(req, nil); ok {
			t.Errorf("%s claimed to place a job with no workers registered", p.Name())
		}
	}
}

func TestNewPolicyAcceptsEveryAdvertisedName(t *testing.T) {
	for _, name := range PolicyNames() {
		if _, err := NewPolicy(name); err != nil {
			t.Errorf("NewPolicy(%q) failed: %v", name, err)
		}
	}
	if _, err := NewPolicy("fastest"); err == nil {
		t.Error("an unknown policy name should be an error, not a silent default")
	}
}

func TestParseOrderingAcceptsEveryAdvertisedName(t *testing.T) {
	for _, name := range OrderingNames() {
		if _, err := ParseOrdering(name); err != nil {
			t.Errorf("ParseOrdering(%q) failed: %v", name, err)
		}
	}
	if _, err := ParseOrdering("lifo"); err == nil {
		t.Error("an unknown ordering should be an error, not a silent default")
	}
}

func TestUtilizationHandlesAnEmptyCluster(t *testing.T) {
	cpu, mem := Utilization(types.Resources{}, types.Resources{})
	if cpu != 0 || mem != 0 {
		t.Fatalf("utilization of an empty cluster = (%v, %v), want zeros rather than NaN", cpu, mem)
	}
}

// TestBackoffIsBoundedAndJittered checks the two properties that matter: the delay
// never exceeds the cap, and repeated calls do not all return the same value (which
// would reconverge every retried job onto the same instant).
func TestBackoffIsBoundedAndJittered(t *testing.T) {
	const base = 100 * time.Millisecond
	const max = 2 * time.Second

	for attempt := int32(1); attempt <= 10; attempt++ {
		window := base << (attempt - 1)
		if window > max {
			window = max
		}
		for i := 0; i < 50; i++ {
			d := backoffDelay(base, max, attempt)
			if d < 0 || d > window {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", attempt, d, window)
			}
		}
	}

	distinct := map[time.Duration]bool{}
	for i := 0; i < 100; i++ {
		distinct[backoffDelay(base, max, 5)] = true
	}
	if len(distinct) < 10 {
		t.Fatalf("backoff produced only %d distinct delays in 100 draws; jitter is not working", len(distinct))
	}
}

func fleetID(fleet []*WorkerView, idx int, ok bool) string {
	if !ok {
		return "<none>"
	}
	return fleet[idx].ID
}

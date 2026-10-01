package scheduler

import (
	"fmt"
	"strings"

	"github.com/BruceMoseti/Atlas/internal/types"
)

// Policy chooses which worker should run a job. Separating it behind an interface is
// what makes the policy comparison in docs/RESULTS.md a controlled experiment: the
// queue, the resource model, the lease machinery, and the workload are identical
// across runs, and only this one decision changes.
type Policy interface {
	Name() string
	// Select returns the index into fleet of the chosen worker. It returns false
	// when no worker can take the job right now.
	Select(req types.Resources, fleet []*WorkerView) (int, bool)
}

// RoundRobin is the deliberately stupid baseline. It hands jobs to workers in
// rotation, skipping any that do not fit. It ignores how loaded a worker is, which is
// precisely what makes it a useful control.
type RoundRobin struct{ cursor int }

func (p *RoundRobin) Name() string { return "round-robin" }

func (p *RoundRobin) Select(req types.Resources, fleet []*WorkerView) (int, bool) {
	n := len(fleet)
	if n == 0 {
		return 0, false
	}
	for i := 0; i < n; i++ {
		idx := (p.cursor + i) % n
		if fleet[idx].Fits(req) {
			p.cursor = (idx + 1) % n
			return idx, true
		}
	}
	return 0, false
}

// LeastLoaded picks the worker with the most free capacity, averaged across CPU and
// memory as a fraction of that worker's own capacity. It spreads load, which keeps
// per-job interference low but tends to leave every worker partially occupied — the
// classic cause of stranded capacity on a heterogeneous fleet.
type LeastLoaded struct{}

func (LeastLoaded) Name() string { return "least-loaded" }

func (LeastLoaded) Select(req types.Resources, fleet []*WorkerView) (int, bool) {
	best, bestScore, found := 0, 0.0, false
	for i, w := range fleet {
		if !w.Fits(req) {
			continue
		}
		cpu, mem := w.freeFraction(types.Resources{})
		score := (cpu + mem) / 2
		if !found || score > bestScore {
			best, bestScore, found = i, score, true
		}
	}
	return best, found
}

// BestFit picks the worker that will have the least capacity left over after the job
// is placed. It packs tightly, which preserves large contiguous holes for large jobs
// at the cost of concentrating load.
type BestFit struct{}

func (BestFit) Name() string { return "best-fit" }

func (BestFit) Select(req types.Resources, fleet []*WorkerView) (int, bool) {
	best, bestScore, found := 0, 0.0, false
	for i, w := range fleet {
		if !w.Fits(req) {
			continue
		}
		cpu, mem := w.freeFraction(req)
		// Lower leftover is better, so negate to keep "higher score wins".
		score := -(cpu + mem)
		if !found || score > bestScore {
			best, bestScore, found = i, score, true
		}
	}
	return best, found
}

// NewPolicy builds a policy by name.
func NewPolicy(name string) (Policy, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "round-robin", "roundrobin", "rr":
		return &RoundRobin{}, nil
	case "least-loaded", "leastloaded", "ll":
		return LeastLoaded{}, nil
	case "best-fit", "bestfit", "bf":
		return BestFit{}, nil
	default:
		return nil, fmt.Errorf("unknown scheduling policy %q (want round-robin, least-loaded, or best-fit)", name)
	}
}

// PolicyNames lists the policies NewPolicy accepts, for CLI help and benchmarks.
func PolicyNames() []string { return []string{"round-robin", "least-loaded", "best-fit"} }

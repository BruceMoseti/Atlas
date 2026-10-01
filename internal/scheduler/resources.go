package scheduler

import "github.com/BruceMoseti/Atlas/internal/types"

// WorkerView is the scheduler's in-memory, placement-oriented picture of a worker.
//
// It is a cache of the worker's row, not the truth. The assignment transaction
// re-reads the worker and re-checks the fit before committing, so a stale view can
// only cost a wasted placement attempt — it can never oversubscribe a worker.
type WorkerView struct {
	ID        string
	Capacity  types.Resources
	Available types.Resources
	Labels    map[string]string

	// Running counts live attempts on this worker. Placement policies use it as a
	// cheap stand-in for queueing at the worker.
	Running int

	// Schedulable mirrors state.WorkerState.Schedulable(): true only for HEALTHY
	// workers that are not draining.
	Schedulable bool
}

// Fits reports whether this worker can accept a request right now. SUSPECT, DEAD,
// and draining workers never fit, regardless of free capacity.
func (w *WorkerView) Fits(r types.Resources) bool {
	return w.Schedulable && r.Fits(w.Available)
}

// freeFraction returns how much of each dimension would remain free after placing r,
// as a fraction of capacity. A zero capacity dimension is treated as fully free so
// that a worker with, say, no declared memory limit is not mis-scored.
func (w *WorkerView) freeFraction(r types.Resources) (cpu, mem float64) {
	cpu, mem = 1, 1
	if w.Capacity.CPUMillis > 0 {
		cpu = float64(w.Available.CPUMillis-r.CPUMillis) / float64(w.Capacity.CPUMillis)
	}
	if w.Capacity.MemoryBytes > 0 {
		mem = float64(w.Available.MemoryBytes-r.MemoryBytes) / float64(w.Capacity.MemoryBytes)
	}
	return cpu, mem
}

// Utilization returns allocated/capacity for a fleet, in [0,1]. A dimension with no
// capacity reports zero rather than NaN.
func Utilization(allocated, capacity types.Resources) (cpu, mem float64) {
	if capacity.CPUMillis > 0 {
		cpu = float64(allocated.CPUMillis) / float64(capacity.CPUMillis)
	}
	if capacity.MemoryBytes > 0 {
		mem = float64(allocated.MemoryBytes) / float64(capacity.MemoryBytes)
	}
	return cpu, mem
}

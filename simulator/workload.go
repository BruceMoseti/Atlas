// Package simulator is a discrete-event model of the Atlas scheduler.
//
// What it does and does not prove is worth being precise about, because
// simulator numbers are easy to overclaim.
//
// It uses the real ready queue, the real placement policies, and the real resource
// model — the same code the server runs. What it replaces is everything below the
// decision: there is no network, no database, no process execution, and the clock is
// virtual. So it measures how a scheduling policy behaves on a given workload and
// fleet shape, at sizes no laptop could actually run.
//
// It does not measure network scalability, durability, or anything about real
// failure timing. Those are what the integration tests and the chaos campaign are
// for.
package simulator

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

// Dist samples a non-negative quantity, used for job runtimes and interarrival
// times.
type Dist interface {
	Sample(r *rand.Rand) float64
	String() string
}

// Fixed always returns the same value.
type Fixed float64

func (f Fixed) Sample(*rand.Rand) float64 { return float64(f) }
func (f Fixed) String() string            { return fmt.Sprintf("fixed(%g)", float64(f)) }

// Uniform samples uniformly from [Lo, Hi].
type Uniform struct{ Lo, Hi float64 }

func (u Uniform) Sample(r *rand.Rand) float64 { return u.Lo + r.Float64()*(u.Hi-u.Lo) }
func (u Uniform) String() string              { return fmt.Sprintf("uniform(%g,%g)", u.Lo, u.Hi) }

// Exponential has the memoryless property, which makes it the standard model for
// arrivals and a reasonable first approximation for service times.
type Exponential struct{ Mean float64 }

func (e Exponential) Sample(r *rand.Rand) float64 { return r.ExpFloat64() * e.Mean }
func (e Exponential) String() string              { return fmt.Sprintf("exp(mean=%g)", e.Mean) }

// Bimodal models the workload shape real batch clusters actually have: a flood of
// short jobs with a minority of long ones. The tail is what makes placement
// decisions matter, because a badly placed long job occupies capacity for a long
// time.
type Bimodal struct {
	ShortMean  float64
	LongMean   float64
	LongWeight float64
}

func (b Bimodal) Sample(r *rand.Rand) float64 {
	if r.Float64() < b.LongWeight {
		return r.ExpFloat64() * b.LongMean
	}
	return r.ExpFloat64() * b.ShortMean
}

func (b Bimodal) String() string {
	return fmt.Sprintf("bimodal(short=%g, long=%g, p_long=%g)", b.ShortMean, b.LongMean, b.LongWeight)
}

// JobClass is one kind of work in a synthetic workload. Classes with explicit
// resource shapes, rather than a single random size, are what let the fragmentation
// experiment ask a specific question: what happens when CPU-heavy and memory-heavy
// jobs compete for a heterogeneous fleet.
type JobClass struct {
	Name        string
	Weight      float64
	CPUMillis   int64
	MemoryBytes int64
	Priority    int32
	// Runtime is in seconds of virtual time.
	Runtime Dist
}

// WorkerClass is a group of identical machines.
type WorkerClass struct {
	Name        string
	Count       int
	CPUMillis   int64
	MemoryBytes int64
}

// pickClass samples a job class by weight.
func pickClass(r *rand.Rand, classes []JobClass) JobClass {
	total := 0.0
	for _, c := range classes {
		total += c.Weight
	}
	x := r.Float64() * total
	for _, c := range classes {
		x -= c.Weight
		if x <= 0 {
			return c
		}
	}
	return classes[len(classes)-1]
}

const gb = int64(1) << 30

// MixedWorkload is the default workload: small, CPU-heavy, and memory-heavy jobs in
// realistic proportions. Most jobs are small; the shaped ones are the minority that
// makes placement interesting.
func MixedWorkload() []JobClass {
	return []JobClass{
		{Name: "small", Weight: 0.60, CPUMillis: 500, MemoryBytes: 512 << 20,
			Runtime: Bimodal{ShortMean: 5, LongMean: 60, LongWeight: 0.05}},
		{Name: "cpu-heavy", Weight: 0.20, CPUMillis: 4000, MemoryBytes: gb,
			Runtime: Bimodal{ShortMean: 20, LongMean: 180, LongWeight: 0.1}},
		{Name: "memory-heavy", Weight: 0.15, CPUMillis: 1000, MemoryBytes: 12 * gb,
			Runtime: Bimodal{ShortMean: 30, LongMean: 200, LongWeight: 0.1}},
		{Name: "large", Weight: 0.05, CPUMillis: 8000, MemoryBytes: 16 * gb,
			Runtime: Exponential{Mean: 120}},
	}
}

// HeterogeneousFleet mixes machine shapes the way a real cluster accumulates them:
// general-purpose nodes, compute-optimized nodes with little memory per core, and
// memory-optimized nodes with lots.
func HeterogeneousFleet(scale int) []WorkerClass {
	if scale < 1 {
		scale = 1
	}
	return []WorkerClass{
		{Name: "general", Count: 5 * scale, CPUMillis: 16000, MemoryBytes: 32 * gb},
		{Name: "compute", Count: 3 * scale, CPUMillis: 32000, MemoryBytes: 16 * gb},
		{Name: "memory", Count: 2 * scale, CPUMillis: 8000, MemoryBytes: 128 * gb},
	}
}

// UniformFleet is the control: every machine identical, so that any difference
// between policies comes from packing rather than from machine shape.
func UniformFleet(count int) []WorkerClass {
	return []WorkerClass{{Name: "uniform", Count: count, CPUMillis: 16000, MemoryBytes: 32 * gb}}
}

// SustainableRate estimates how many jobs per second a fleet can retain, as the
// tighter of the CPU and memory bounds.
//
// It is an upper bound on what perfect packing could achieve, which makes it the
// right denominator for "offered load": a run at 90% of this number is genuinely
// near capacity, and any queueing that appears there is the packing losing ground
// rather than the cluster simply being too small.
func SustainableRate(fleet []WorkerClass, classes []JobClass) float64 {
	var fleetCPU, fleetMem float64
	for _, c := range fleet {
		fleetCPU += float64(c.Count) * float64(c.CPUMillis)
		fleetMem += float64(c.Count) * float64(c.MemoryBytes)
	}

	var weight, cpuSeconds, memSeconds float64
	for _, c := range classes {
		weight += c.Weight
	}
	for _, c := range classes {
		p := c.Weight / weight
		mean := meanOf(c.Runtime)
		cpuSeconds += p * float64(c.CPUMillis) * mean
		memSeconds += p * float64(c.MemoryBytes) * mean
	}

	rate := math.Inf(1)
	if cpuSeconds > 0 {
		rate = math.Min(rate, fleetCPU/cpuSeconds)
	}
	if memSeconds > 0 {
		rate = math.Min(rate, fleetMem/memSeconds)
	}
	if math.IsInf(rate, 1) {
		return 0
	}
	return rate
}

// meanOf returns a distribution's expected value. Only the shapes this package
// defines are supported, which is all SustainableRate needs.
func meanOf(d Dist) float64 {
	switch v := d.(type) {
	case Fixed:
		return float64(v)
	case Uniform:
		return (v.Lo + v.Hi) / 2
	case Exponential:
		return v.Mean
	case Bimodal:
		return v.LongWeight*v.LongMean + (1-v.LongWeight)*v.ShortMean
	default:
		return 1
	}
}

func describeClasses(classes []JobClass) string {
	parts := make([]string, 0, len(classes))
	for _, c := range classes {
		parts = append(parts, fmt.Sprintf("%s %.0f%% (%s cpu, %s mem, runtime %s)",
			c.Name, c.Weight*100, formatCPU(c.CPUMillis), formatBytes(c.MemoryBytes), c.Runtime))
	}
	return strings.Join(parts, "; ")
}

func describeFleet(classes []WorkerClass) string {
	parts := make([]string, 0, len(classes))
	total := 0
	for _, c := range classes {
		total += c.Count
		parts = append(parts, fmt.Sprintf("%d x %s (%s cpu, %s mem)",
			c.Count, c.Name, formatCPU(c.CPUMillis), formatBytes(c.MemoryBytes)))
	}
	return fmt.Sprintf("%d workers: %s", total, strings.Join(parts, "; "))
}

func formatCPU(millis int64) string { return fmt.Sprintf("%g", float64(millis)/1000) }

func formatBytes(n int64) string {
	switch {
	case n >= gb:
		return fmt.Sprintf("%gGiB", math.Round(float64(n)/float64(gb)*10)/10)
	case n >= 1<<20:
		return fmt.Sprintf("%gMiB", math.Round(float64(n)/float64(1<<20)*10)/10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func seconds(d time.Duration) float64 { return d.Seconds() }

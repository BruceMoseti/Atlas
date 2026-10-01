package simulator

import (
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// The experiments in this file are the ones reported in docs/RESULTS.md. Each one
// changes exactly one variable and holds everything else fixed, including the random
// seed, so the differences between rows are attributable.

// PolicyComparison runs the same workload under each placement policy, at several
// offered loads.
//
// Load is the variable that decides whether placement quality matters at all. On an
// empty cluster every policy puts the job somewhere and the job runs; under a
// permanently saturated queue the backfill window finds something that fits no
// matter how badly the last decision went. The interesting regime is near capacity,
// where a bad placement is the difference between a job starting now and a job
// waiting for something else to finish.
func PolicyComparison(w io.Writer, workers, jobs int, seed int64) []Result {
	fleet := HeterogeneousFleet(workers / 10)
	classes := MixedWorkload()
	capacityRate := SustainableRate(fleet, classes)

	header(w, "Experiment 1 - placement policy versus offered load")
	fmt.Fprintf(w, "%s\n", describeFleet(fleet))
	fmt.Fprintf(w, "classes: %s\n", describeClasses(classes))
	fmt.Fprintf(w, "estimated sustainable throughput: %.1f jobs/sec\n", capacityRate)
	fmt.Fprintf(w, "workload: %d jobs per run, Poisson arrivals, seed %d\n\n", jobs, seed)

	var results []Result
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "LOAD\tPOLICY\tMEAN WAIT\tP50\tP95\tP99\tCPU UTIL\tMEM UTIL\tSTRANDED CPU\tSTRANDED MEM\tQUEUE PEAK")
	for _, load := range []float64{0.70, 0.90, 1.05} {
		for _, name := range scheduler.PolicyNames() {
			policy, err := scheduler.NewPolicy(name)
			if err != nil {
				panic(err)
			}
			r := Run(Config{
				Fleet:       fleet,
				JobClasses:  classes,
				Jobs:        jobs,
				ArrivalRate: capacityRate * load,
				Policy:      policy,
				Ordering:    scheduler.OrderFIFO,
				Seed:        seed,
			})
			results = append(results, r)
			fmt.Fprintf(tw, "%.0f%%\t%s\t%s\t%s\t%s\t%s\t%.1f%%\t%.1f%%\t%.1f%%\t%.1f%%\t%d\n",
				load*100, r.Policy, dur(r.WaitMean), dur(r.WaitP50), dur(r.WaitP95), dur(r.WaitP99),
				r.CPUUtilization*100, r.MemUtilization*100,
				r.StrandedCPU*100, r.StrandedMemory*100, r.PeakQueueDepth)
		}
	}
	tw.Flush()

	fmt.Fprintf(w, "\nStranded capacity is the mean fraction of the fleet that sat free while at\n")
	fmt.Fprintf(w, "least one job was waiting for capacity, averaged over exactly that time. It\n")
	fmt.Fprintf(w, "is fragmentation made measurable: resources that existed, were idle, and\n")
	fmt.Fprintf(w, "still could not be used. Lower is better.\n")
	return results
}

// Fragmentation isolates the heterogeneity effect by running the same workload on a
// uniform fleet and a heterogeneous one of identical total capacity.
//
// Holding the totals equal is the point. Any difference between the two rows is
// caused by the shape of the machines, not by how many resources exist.
func Fragmentation(w io.Writer, jobs int, seed int64) []Result {
	header(w, "Experiment 1b - resource fragmentation on a heterogeneous fleet")

	hetero := HeterogeneousFleet(2)
	var totalCPU, totalMem int64
	count := 0
	for _, c := range hetero {
		totalCPU += int64(c.Count) * c.CPUMillis
		totalMem += int64(c.Count) * c.MemoryBytes
		count += c.Count
	}
	uniform := []WorkerClass{{
		Name: "uniform", Count: count,
		CPUMillis:   totalCPU / int64(count),
		MemoryBytes: totalMem / int64(count),
	}}

	classes := MixedWorkload()
	rate := SustainableRate(hetero, classes) * 0.9

	fmt.Fprintf(w, "both fleets hold %s cpu and %s memory in total, across %d machines\n",
		formatCPU(totalCPU), formatBytes(totalMem), count)
	fmt.Fprintf(w, "  uniform:       %s\n", describeFleet(uniform))
	fmt.Fprintf(w, "  heterogeneous: %s\n", describeFleet(hetero))
	fmt.Fprintf(w, "workload: %d jobs at %.1f/sec (90%% of estimated capacity)\n", jobs, rate)
	fmt.Fprintf(w, "classes: %s\n\n", describeClasses(classes))

	var results []Result
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "FLEET\tPOLICY\tMEAN WAIT\tP99 WAIT\tCPU UTIL\tMEM UTIL\tSTRANDED CPU\tSTRANDED MEM")
	for _, fleet := range []struct {
		name    string
		classes []WorkerClass
	}{{"uniform", uniform}, {"heterogeneous", hetero}} {
		for _, name := range scheduler.PolicyNames() {
			policy, _ := scheduler.NewPolicy(name)
			r := Run(Config{
				Fleet:       fleet.classes,
				JobClasses:  classes,
				Jobs:        jobs,
				ArrivalRate: rate,
				Policy:      policy,
				Ordering:    scheduler.OrderFIFO,
				Seed:        seed,
			})
			results = append(results, r)
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.1f%%\t%.1f%%\t%.1f%%\t%.1f%%\n",
				fleet.name, r.Policy, dur(r.WaitMean), dur(r.WaitP99),
				r.CPUUtilization*100, r.MemUtilization*100, r.StrandedCPU*100, r.StrandedMemory*100)
		}
	}
	tw.Flush()
	return results
}

// Scalability measures placement-decision cost as the fleet grows.
//
// This is the one experiment about the implementation rather than the model, so it
// deliberately leaves the simulation out: a fleet is built at a controlled
// occupancy and Policy.Select is called directly, several hundred thousand times.
// Running it through the event loop instead would mix in queue behaviour and make
// the per-decision number depend on how long the queue happened to be.
//
// The question it answers is narrow and worth answering precisely: every policy
// scans the fleet linearly, so at what fleet size does that start to hurt?
func Scalability(w io.Writer, decisions int, seed int64) []ScaleResult {
	if decisions <= 0 {
		decisions = 200000
	}
	header(w, "Experiment 5 - placement decision cost versus fleet size")
	fmt.Fprintf(w, "Policy.Select is called directly against a synthetic fleet; no queue, no\n")
	fmt.Fprintf(w, "event loop, no database. %d decisions per row.\n\n", decisions)

	scenarios := []struct {
		name      string
		occupancy float64
		request   types.Resources
	}{
		{"idle, small job", 0.0, types.Resources{CPUMillis: 500, MemoryBytes: 512 << 20}},
		{"busy, small job", 0.8, types.Resources{CPUMillis: 500, MemoryBytes: 512 << 20}},
		// Nothing in the fleet can hold this, so every policy is forced to
		// examine every worker. It is the dispatcher's worst case and a real
		// one: a large job at the head of the queue on a busy cluster.
		{"busy, job fits nowhere", 0.8, types.Resources{CPUMillis: 15000, MemoryBytes: 30 * gb}},
	}

	var results []ScaleResult
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "WORKERS\tSCENARIO\tPOLICY\tMEAN\tP50\tP99\tDECISIONS/SEC")
	for _, n := range []int{10, 100, 1000, 10000} {
		for _, sc := range scenarios {
			for _, name := range scheduler.PolicyNames() {
				policy, _ := scheduler.NewPolicy(name)
				r := measureSelect(policy, n, sc.occupancy, sc.request, decisions, seed)
				r.Scenario = sc.name
				results = append(results, r)
				fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%.0f\n",
					n, sc.name, r.Policy, r.Mean, r.P50, r.P99, r.DecisionsPerSecond)
			}
		}
	}
	tw.Flush()

	fmt.Fprintf(w, "\nThree things to read out of this table.\n\n")
	fmt.Fprintf(w, "Least-loaded and best-fit are strictly linear in fleet size: they score every\n")
	fmt.Fprintf(w, "worker before choosing, so their cost tracks worker count and nothing else.\n\n")
	fmt.Fprintf(w, "Round-robin's cost depends entirely on how soon it finds a worker that fits.\n")
	fmt.Fprintf(w, "When one does, it is flat regardless of fleet size. When none does, it scans\n")
	fmt.Fprintf(w, "everything and becomes the most expensive policy in the table.\n\n")
	fmt.Fprintf(w, "The 'fits nowhere' rows are cheaper per worker for the scoring policies than\n")
	fmt.Fprintf(w, "the rows where placement succeeds, because a worker that fails the fit check\n")
	fmt.Fprintf(w, "is never scored. The expensive case for a scoring policy is a successful\n")
	fmt.Fprintf(w, "placement, not a failed one.\n")
	return results
}

// ScaleResult is one row of the decision-cost benchmark.
type ScaleResult struct {
	Workers            int
	Occupancy          float64
	Scenario           string
	Policy             string
	Decisions          int
	Mean               time.Duration
	P50                time.Duration
	P99                time.Duration
	DecisionsPerSecond float64
}

// measureSelect times Policy.Select against a fleet at a fixed occupancy.
func measureSelect(policy scheduler.Policy, workers int, occupancy float64, req types.Resources, decisions int, seed int64) ScaleResult {
	rng := rand.New(rand.NewSource(seed))

	const capCPU, capMem = int64(16000), 32 * gb
	fleet := make([]*scheduler.WorkerView, workers)
	for i := range fleet {
		capacity := types.Resources{CPUMillis: capCPU, MemoryBytes: capMem}
		// Jitter the occupancy so the fleet is not uniform, which would let a
		// policy get lucky on the first worker every time.
		used := occupancy * (0.5 + rng.Float64())
		if used > 0.95 {
			used = 0.95
		}
		fleet[i] = &scheduler.WorkerView{
			ID:       fmt.Sprintf("w-%d", i),
			Capacity: capacity,
			Available: types.Resources{
				CPUMillis:   capCPU - int64(float64(capCPU)*used),
				MemoryBytes: capMem - int64(float64(capMem)*used),
			},
			Schedulable: true,
		}
	}

	samples := make([]time.Duration, decisions)
	start := time.Now()
	for i := 0; i < decisions; i++ {
		t0 := time.Now()
		policy.Select(req, fleet)
		samples[i] = time.Since(t0)
	}
	total := time.Since(start)

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var sum time.Duration
	for _, s := range samples {
		sum += s
	}

	return ScaleResult{
		Workers:            workers,
		Occupancy:          occupancy,
		Policy:             policy.Name(),
		Decisions:          decisions,
		Mean:               sum / time.Duration(decisions),
		P50:                samples[decisions/2],
		P99:                samples[int(float64(decisions)*0.99)],
		DecisionsPerSecond: float64(decisions) / total.Seconds(),
	}
}

// Overload raises the arrival rate past what the cluster can serve, with and without
// admission control, and reports what each one does to the caller.
func Overload(w io.Writer, jobs int, seed int64) []Result {
	header(w, "Experiment 4 - overload, with and without admission control")

	fleet := UniformFleet(20)
	classes := []JobClass{
		{Name: "uniform", Weight: 1, CPUMillis: 2000, MemoryBytes: 4 * gb, Runtime: Exponential{Mean: 10}},
	}
	// 20 workers x 16 cores / 2 cores per job = 160 concurrent jobs, each
	// averaging 10s, so the cluster can sustain about 16 jobs/sec.
	fmt.Fprintf(w, "%s\n", describeFleet(fleet))
	fmt.Fprintf(w, "workload: %d jobs of 2 cpu / 4GiB, runtime exp(mean=10s)\n", jobs)
	fmt.Fprintf(w, "sustainable throughput is about 16 jobs/sec, so rates above that are overload\n\n")

	var results []Result
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ARRIVAL/S\tADMISSION\tACCEPTED\tREJECTED\tPEAK QUEUE\tMEAN WAIT\tP99 WAIT\tCPU UTIL")
	for _, rate := range []float64{8, 16, 32, 64, 128} {
		for _, adm := range []struct {
			label string
			depth int
		}{{"none", 0}, {"queue<=500", 500}} {
			r := Run(Config{
				Fleet:         fleet,
				JobClasses:    classes,
				Jobs:          jobs,
				ArrivalRate:   rate,
				Policy:        scheduler.BestFit{},
				Ordering:      scheduler.OrderFIFO,
				MaxQueueDepth: adm.depth,
				Seed:          seed,
			})
			results = append(results, r)
			fmt.Fprintf(tw, "%.0f\t%s\t%d\t%d\t%d\t%s\t%s\t%.1f%%\n",
				rate, adm.label, r.JobsAccepted, r.JobsRejected, r.PeakQueueDepth,
				dur(r.WaitMean), dur(r.WaitP99), r.CPUUtilization*100)
		}
	}
	tw.Flush()

	fmt.Fprintf(w, "\nWithout admission control an overloaded cluster still accepts everything, and\n")
	fmt.Fprintf(w, "the cost lands on wait time, which the caller cannot see until it is already\n")
	fmt.Fprintf(w, "enormous. With a bounded queue the same overload produces an immediate\n")
	fmt.Fprintf(w, "RESOURCE_EXHAUSTED, which the caller can act on.\n")
	return results
}

// PriorityAging compares strict priority against priority with aging on a workload
// where high-priority jobs never stop arriving, which is the condition under which
// strict priority starves everything else.
func PriorityAging(w io.Writer, jobs int, seed int64) []Result {
	header(w, "Experiment 6 - priority aging versus starvation")

	classes := []JobClass{
		{Name: "urgent", Weight: 0.7, Priority: 100, CPUMillis: 2000, MemoryBytes: 4 * gb,
			Runtime: Exponential{Mean: 5}},
		{Name: "batch", Weight: 0.3, Priority: 0, CPUMillis: 2000, MemoryBytes: 4 * gb,
			Runtime: Exponential{Mean: 5}},
	}
	fleet := UniformFleet(10)
	fmt.Fprintf(w, "%s\n", describeFleet(fleet))
	fmt.Fprintf(w, "workload: %d jobs, 70%% at priority 100 and 30%% at priority 0, arriving at 40/sec\n", jobs)
	fmt.Fprintf(w, "the cluster is deliberately undersized, so low-priority work only runs if the\n")
	fmt.Fprintf(w, "ordering lets it\n\n")

	type variant struct {
		label     string
		ordering  scheduler.Ordering
		agingRate float64
		agingCap  float64
	}
	// The capped and uncapped 10/s rows exist to isolate one specific trap: a
	// cap that every queued job eventually reaches restores strict priority,
	// because once both classes are pinned at the maximum bonus the only thing
	// left separating them is their base priority again.
	variants := []variant{
		{"fifo (priority ignored)", scheduler.OrderFIFO, 0, 0},
		{"strict priority", scheduler.OrderPriority, 0, 0},
		{"priority + aging (1/s, cap 1000)", scheduler.OrderPriorityAging, 1, 1000},
		{"priority + aging (10/s, cap 1000)", scheduler.OrderPriorityAging, 10, 1000},
		{"priority + aging (10/s, no cap)", scheduler.OrderPriorityAging, 10, 0},
	}

	var results []Result
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ORDERING\tCLASS\tMEAN WAIT\tP50 WAIT\tP99 WAIT\tMAX WAIT\tCPU UTIL")
	for _, v := range variants {
		r := Run(Config{
			Fleet:       fleet,
			JobClasses:  classes,
			Jobs:        jobs,
			ArrivalRate: 40,
			Policy:      scheduler.BestFit{},
			Ordering:    v.ordering,
			AgingRate:   v.agingRate,
			AgingCap:    v.agingCap,
			Seed:        seed,
		})
		results = append(results, r)
		for _, class := range []string{"urgent", "batch"} {
			cs := r.ByClass[class]
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%.1f%%\n",
				v.label, class, dur(cs.WaitMean), dur(cs.WaitP50),
				dur(cs.WaitP99), dur(cs.WaitMax), r.CPUUtilization*100)
		}
	}
	tw.Flush()

	fmt.Fprintf(w, "\nThe aggregate wait hides the whole story here, which is why the table is split\n")
	fmt.Fprintf(w, "by class. Read the gap between the urgent and batch rows: strict priority buys\n")
	fmt.Fprintf(w, "urgent work a shorter wait and charges batch work for it, and aging is the\n")
	fmt.Fprintf(w, "knob that decides how large that bill is allowed to get.\n")
	return results
}

// WorkerFailure measures what losing machines costs, in the simulator's terms.
func WorkerFailure(w io.Writer, jobs int, seed int64) []Result {
	header(w, "Experiment 2b - worker failure rate versus completion time")
	fmt.Fprintf(w, "failures per worker per simulated hour; a failed worker loses everything it\n")
	fmt.Fprintf(w, "was running, and that work is not requeued until its lease would have expired\n\n")

	fleet := UniformFleet(50)
	var results []Result
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "FAILURES/WORKER/HR\tFAILURES\tLOST ATTEMPTS\tRETRIES\tCOMPLETED\tABANDONED\tMEAN TURNAROUND\tP99 TURNAROUND")
	for _, rate := range []float64{0, 1, 10, 60} {
		r := Run(Config{
			Fleet:             fleet,
			JobClasses:        MixedWorkload(),
			Jobs:              jobs,
			ArrivalRate:       20,
			Policy:            scheduler.BestFit{},
			Ordering:          scheduler.OrderFIFO,
			WorkerFailureRate: rate,
			WorkerRecovery:    30 * time.Second,
			LeaseTTL:          15 * time.Second,
			MaxAttempts:       5,
			Seed:              seed,
		})
		results = append(results, r)
		fmt.Fprintf(tw, "%.0f\t%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
			rate, r.WorkerFailures, r.LostAttempts, r.Retries, r.JobsCompleted,
			r.JobsAbandoned, dur(r.TurnaroundMean), dur(r.TurnaroundP99))
	}
	tw.Flush()
	return results
}

func header(w io.Writer, title string) {
	fmt.Fprintf(w, "\n%s\n%s\n\n", title, strings.Repeat("=", len(title)))
}

// dur renders seconds of virtual time compactly.
func dur(sec float64) string {
	switch {
	case sec < 1:
		return fmt.Sprintf("%.0fms", sec*1000)
	case sec < 120:
		return fmt.Sprintf("%.2fs", sec)
	case sec < 7200:
		return fmt.Sprintf("%.1fm", sec/60)
	default:
		return fmt.Sprintf("%.1fh", sec/3600)
	}
}

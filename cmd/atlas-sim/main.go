// Command atlas-sim runs the Atlas scheduler simulation experiments.
//
// The simulator drives the real ready queue, placement policies, and resource model
// against a virtual clock and a synthetic fleet. It measures scheduling behaviour at
// sizes no single machine could actually run. It does not measure the network, the
// database, or real failure timing; see docs/BENCHMARKING.md for what each number
// does and does not mean.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/BruceMoseti/Atlas/simulator"
)

const usage = `atlas-sim - scheduler simulation experiments

Usage:
  atlas-sim <experiment> [flags]

Experiments:
  policy          compare round-robin, least-loaded, and best-fit placement
  fragmentation   uniform versus heterogeneous fleets of equal total capacity
  scale           placement decision cost at 10, 100, 1k, and 10k workers
  overload        arrival rates past capacity, with and without admission control
  priority        strict priority versus priority with aging
  failure         worker failure rate versus job turnaround
  all             every experiment above

Flags:
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "atlas-sim:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("atlas-sim", flag.ExitOnError)
	workers := fs.Int("workers", 100, "fleet size for the policy experiment")
	jobs := fs.Int("jobs", 20000, "jobs per run")
	decisions := fs.Int("decisions", 200000, "placement decisions per row of the scale experiment")
	seed := fs.Int64("seed", 1, "random seed; runs are reproducible for a given seed")
	out := fs.String("out", "", "also write the report to this file")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		fs.PrintDefaults()
	}

	if len(os.Args) < 2 {
		fs.Usage()
		return fmt.Errorf("an experiment name is required")
	}
	experiment := os.Args[1]
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}

	var w io.Writer = os.Stdout
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			return err
		}
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = io.MultiWriter(os.Stdout, f)
	}

	fmt.Fprint(w, simulator.Environment())

	switch strings.ToLower(experiment) {
	case "policy":
		simulator.PolicyComparison(w, *workers, *jobs, *seed)
	case "fragmentation":
		simulator.Fragmentation(w, *jobs, *seed)
	case "scale":
		simulator.Scalability(w, *decisions, *seed)
	case "overload":
		simulator.Overload(w, *jobs, *seed)
	case "priority":
		simulator.PriorityAging(w, *jobs, *seed)
	case "failure":
		simulator.WorkerFailure(w, *jobs, *seed)
	case "all":
		simulator.PolicyComparison(w, *workers, *jobs, *seed)
		simulator.Fragmentation(w, *jobs, *seed)
		simulator.PriorityAging(w, *jobs, *seed)
		simulator.Overload(w, *jobs, *seed)
		simulator.WorkerFailure(w, *jobs, *seed)
		simulator.Scalability(w, *decisions, *seed)
	case "help", "-h", "--help":
		fs.Usage()
		return nil
	default:
		fs.Usage()
		return fmt.Errorf("unknown experiment %q", experiment)
	}
	return nil
}

// Command atlas-chaos runs a randomized fault-injection campaign against a real
// Atlas cluster and reports whether the documented invariants survived it.
//
// Everything it starts is a real process with a real database. The faults are
// SIGKILL, SIGSTOP, scheduler restarts, dropped heartbeats, dropped lease renewals,
// and replayed RPCs. It exits non-zero if any invariant was violated, so it works
// as a CI gate rather than only as a demo.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/BruceMoseti/Atlas/chaos"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "atlas-chaos:", err)
		os.Exit(2)
	}
}

func run() error {
	var cfg chaos.Config
	var reportPath string

	flag.StringVar(&cfg.BinDir, "bin", "bin", "directory holding atlas-server and atlas-worker")
	flag.StringVar(&cfg.WorkDir, "workdir", "", "directory for the database, logs, and report (default results/chaos-TIMESTAMP)")
	flag.StringVar(&cfg.GRPCAddr, "listen", "127.0.0.1:50551", "gRPC address for the scheduler under test")
	flag.StringVar(&cfg.HTTPAddr, "http", "127.0.0.1:59090", "HTTP address for the scheduler under test")

	flag.IntVar(&cfg.Workers, "workers", 6, "number of worker processes")
	flag.IntVar(&cfg.Jobs, "jobs", 500, "number of jobs to submit")
	flag.DurationVar(&cfg.Duration, "duration", 60*time.Second, "length of the fault-injection window")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", 3*time.Minute, "how long to wait for the cluster to drain afterwards")
	flag.DurationVar(&cfg.JobDuration, "job-duration", 300*time.Millisecond, "how long each workload sleeps")
	flag.IntVar(&cfg.MaxAttempts, "max-attempts", 8, "attempt budget per job")

	flag.BoolVar(&cfg.KillWorkers, "kill-workers", true, "SIGKILL random workers and restart them")
	flag.BoolVar(&cfg.PauseWorkers, "pause-workers", true, "SIGSTOP random workers, so they hold leases while answering nothing")
	flag.BoolVar(&cfg.RestartScheduler, "restart-scheduler", false, "SIGKILL and restart the scheduler")
	flag.Float64Var(&cfg.DuplicateRPCRate, "duplicate-rpc-rate", 0, "probability a worker replays each completion report")
	flag.Float64Var(&cfg.HeartbeatDropRate, "heartbeat-drop-rate", 0, "probability a worker skips each heartbeat")
	flag.Float64Var(&cfg.RenewDropRate, "renew-drop-rate", 0, "probability a worker skips each lease renewal")
	flag.DurationVar(&cfg.FaultInterval, "fault-interval", 3*time.Second, "mean time between injected faults")

	flag.DurationVar(&cfg.LeaseTTL, "lease-ttl", 4*time.Second, "lease TTL for the scheduler under test")
	flag.DurationVar(&cfg.SuspectAfter, "suspect-after", 1500*time.Millisecond, "heartbeat age at which a worker stops receiving work")
	flag.DurationVar(&cfg.DeadAfter, "dead-after", 4*time.Second, "heartbeat age at which a worker's leases are reclaimed")

	flag.Int64Var(&cfg.Seed, "seed", 0, "random seed (0 picks one and prints it)")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "echo child process logs to stderr")
	flag.StringVar(&reportPath, "report", "", "also write the report to this path")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	report, err := chaos.Run(ctx, cfg)
	if report == nil {
		return err
	}

	var w io.Writer = os.Stdout
	if reportPath != "" {
		if dir := filepath.Dir(reportPath); dir != "" {
			if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
				return mkErr
			}
		}
		f, ferr := os.Create(reportPath)
		if ferr != nil {
			return ferr
		}
		defer f.Close()
		w = io.MultiWriter(os.Stdout, f)
	}
	chaos.WriteReport(w, report)

	if err != nil {
		return err
	}
	if report.InvariantsRep == nil || !report.InvariantsRep.OK() {
		os.Exit(1)
	}
	if !report.Drained {
		os.Exit(1)
	}
	return nil
}

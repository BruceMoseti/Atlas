// Command atlas-worker runs an Atlas execution agent.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/BruceMoseti/Atlas/internal/logging"
	"github.com/BruceMoseti/Atlas/internal/types"
	"github.com/BruceMoseti/Atlas/internal/worker"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "atlas-worker:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		schedulerAddr = flag.String("scheduler", "localhost:50051", "scheduler gRPC address")
		workerID      = flag.String("id", "", "worker id (defaults to hostname)")
		cpu           = flag.String("cpu", "4", "CPU capacity in cores, e.g. 4 or 2.5")
		memory        = flag.String("memory", "8GB", "memory capacity, e.g. 512MB, 8GB")
		labels        = flag.String("labels", "", "comma-separated key=value labels")
		executorName  = flag.String("executor", "process", "process or docker")
		dockerNetwork = flag.String("docker-network", "none", "value passed to docker run --network")
		acquireWait   = flag.Duration("acquire-wait", 5*time.Second, "long-poll budget per AcquireJob call")

		dupRPC       = flag.Float64("chaos-duplicate-rpc-rate", 0, "probability of replaying each completion report")
		dropHB       = flag.Float64("chaos-heartbeat-drop-rate", 0, "probability of skipping each heartbeat")
		dropRenew    = flag.Float64("chaos-renew-drop-rate", 0, "probability of skipping each lease renewal")
		exitAfter    = flag.Duration("chaos-exit-after", 0, "exit abruptly after this long (0 disables)")
		logLevel     = flag.String("log-level", "info", "debug, info, warn, or error")
		logJSON      = flag.Bool("log-json", false, "emit JSON logs instead of logfmt")
		printVersion = flag.Bool("version", false, "print the worker version and exit")
	)
	flag.Parse()

	const version = "0.1.0"
	if *printVersion {
		fmt.Println(version)
		return nil
	}

	log := logging.New(logging.Options{Level: *logLevel, JSON: *logJSON})

	id := *workerID
	if id == "" {
		h, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("could not determine hostname for worker id: %w", err)
		}
		id = h
	}

	cpuMillis, err := types.ParseCPUMillis(*cpu)
	if err != nil {
		return err
	}
	memBytes, err := types.ParseBytes(*memory)
	if err != nil {
		return err
	}
	parsedLabels, err := parseLabels(*labels)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var exec worker.Executor
	switch strings.ToLower(*executorName) {
	case "process":
		exec = worker.ProcessExecutor{}
	case "docker":
		d := worker.DockerExecutor{Network: *dockerNetwork}
		// Fail now rather than failing every job: a worker that cannot run
		// containers should not advertise capacity for them.
		if err := d.Available(ctx); err != nil {
			return err
		}
		exec = d
	default:
		return fmt.Errorf("unknown executor %q (want process or docker)", *executorName)
	}

	conn, err := grpc.NewClient(*schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial scheduler %s: %w", *schedulerAddr, err)
	}
	defer conn.Close()

	w, err := worker.New(pb.NewWorkerServiceClient(conn), worker.Config{
		WorkerID:          id,
		Version:           version,
		Labels:            parsedLabels,
		Capacity:          types.Resources{CPUMillis: cpuMillis, MemoryBytes: memBytes},
		Executor:          exec,
		Logger:            log,
		AcquireWait:       *acquireWait,
		DuplicateRPCRate:  *dupRPC,
		HeartbeatDropRate: *dropHB,
		RenewDropRate:     *dropRenew,
	})
	if err != nil {
		return err
	}

	if *exitAfter > 0 {
		// Used by the chaos harness to make a worker disappear without a
		// SIGTERM, imitating a kernel panic or a yanked power cable.
		go func() {
			time.Sleep(*exitAfter)
			log.Warn("chaos_abrupt_exit", "after", exitAfter.String())
			os.Exit(137)
		}()
	}

	return w.Run(ctx)
}

func parseLabels(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			return nil, fmt.Errorf("invalid label %q: want key=value", pair)
		}
		out[k] = v
	}
	return out, nil
}

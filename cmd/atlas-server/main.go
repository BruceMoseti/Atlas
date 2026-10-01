// Command atlas-server runs the Atlas control plane: the gRPC API, the scheduler,
// and the operational HTTP surface.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BruceMoseti/Atlas/internal/api"
	"github.com/BruceMoseti/Atlas/internal/logging"
	"github.com/BruceMoseti/Atlas/internal/metrics"
	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/store"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "atlas-server:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		grpcAddr = flag.String("listen", ":50051", "gRPC listen address")
		httpAddr = flag.String("http", ":9090", "HTTP address for /metrics, /live, /ready, /status")
		dbPath   = flag.String("db", "atlas.db", "path to the SQLite state file")

		policyName   = flag.String("policy", "best-fit", "placement policy: "+joinNames(scheduler.PolicyNames()))
		orderingName = flag.String("ordering", "priority-aging", "queue ordering: "+joinNames(scheduler.OrderingNames()))
		agingRate    = flag.Float64("aging-rate", 1.0, "priority points gained per second of queue wait")
		agingCap     = flag.Float64("aging-cap", 50, "maximum priority gain from aging")

		leaseTTL     = flag.Duration("lease-ttl", 15*time.Second, "how long an assignment stays valid without renewal")
		heartbeat    = flag.Duration("heartbeat-interval", 2*time.Second, "heartbeat interval advertised to workers")
		suspectAfter = flag.Duration("suspect-after", 6*time.Second, "heartbeat age at which a worker stops receiving new work")
		deadAfter    = flag.Duration("dead-after", 15*time.Second, "heartbeat age at which a worker's assignments are reclaimed")

		dispatchInterval  = flag.Duration("dispatch-interval", 25*time.Millisecond, "maximum delay before a dispatchable job is considered")
		reconcileInterval = flag.Duration("reconcile-interval", 250*time.Millisecond, "lease and health sweep interval")
		dispatchBatch     = flag.Int("dispatch-batch", 256, "placements committed per transaction")

		retryBase = flag.Duration("retry-base-delay", 250*time.Millisecond, "base delay for retry backoff")
		retryMax  = flag.Duration("retry-max-delay", 30*time.Second, "maximum delay for retry backoff")

		maxQueue       = flag.Int("max-queue-depth", 100000, "admission control: maximum queued jobs (0 disables)")
		maxPerClient   = flag.Int("max-inflight-per-client", 10000, "admission control: maximum non-terminal jobs per client (0 disables)")
		maxJobCPU      = flag.Int64("max-job-cpu-millis", 0, "admission control: largest CPU request accepted (0 disables)")
		maxJobMem      = flag.Int64("max-job-memory-bytes", 0, "admission control: largest memory request accepted (0 disables)")
		rejectUnsched  = flag.Bool("reject-unschedulable", true, "refuse jobs no registered worker could ever run")
		defaultRetries = flag.Int("default-max-attempts", 3, "attempt budget applied when a submission omits one")

		logLevel = flag.String("log-level", "info", "debug, info, warn, or error")
		logJSON  = flag.Bool("log-json", false, "emit JSON logs instead of logfmt")
	)
	flag.Parse()

	log := logging.New(logging.Options{Level: *logLevel, JSON: *logJSON})

	policy, err := scheduler.NewPolicy(*policyName)
	if err != nil {
		return err
	}
	ordering, err := scheduler.ParseOrdering(*orderingName)
	if err != nil {
		return err
	}

	st, err := store.Open(store.Options{Path: *dbPath})
	if err != nil {
		return err
	}
	defer st.Close()

	reg := prometheus.NewRegistry()
	met := metrics.New(reg)

	sched := scheduler.New(st, scheduler.Config{
		Policy: policy,
		Queue: scheduler.QueueConfig{
			Ordering:  ordering,
			AgingRate: *agingRate,
			AgingCap:  *agingCap,
		},
		LeaseTTL:          *leaseTTL,
		HeartbeatInterval: *heartbeat,
		SuspectAfter:      *suspectAfter,
		DeadAfter:         *deadAfter,
		DispatchInterval:  *dispatchInterval,
		ReconcileInterval: *reconcileInterval,
		MaxDispatchBatch:  *dispatchBatch,
		RetryBaseDelay:    *retryBase,
		RetryMaxDelay:     *retryMax,
		Admission: scheduler.AdmissionConfig{
			MaxQueueDepth:        *maxQueue,
			MaxInFlightPerClient: *maxPerClient,
			MaxJobCPUMillis:      *maxJobCPU,
			MaxJobMemoryBytes:    *maxJobMem,
			RejectUnschedulable:  *rejectUnsched,
			DefaultMaxAttempts:   int32(*defaultRetries),
		},
		Logger:  log,
		Metrics: met,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := sched.Start(ctx); err != nil {
		return err
	}
	defer sched.Stop()

	srv := grpc.NewServer()
	apiServer := api.NewServer(sched)
	pb.RegisterAtlasServiceServer(srv, apiServer)
	pb.RegisterWorkerServiceServer(srv, apiServer)

	// The standard gRPC health protocol, so load balancers and clients can skip
	// an unhealthy backend without Atlas inventing its own convention.
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("atlas.v1.AtlasService", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("atlas.v1.WorkerService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", *grpcAddr, err)
	}

	httpSrv := &http.Server{
		Addr:              *httpAddr,
		Handler:           api.NewHealthHandler(st, sched, reg),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http_server_failed", "error", err)
		}
	}()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	log.Info("scheduler_listening",
		"grpc", *grpcAddr, "http", *httpAddr, "db", *dbPath,
		"policy", policy.Name(), "ordering", string(ordering),
		"lease_ttl_ms", leaseTTL.Milliseconds())

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	// Graceful shutdown: stop accepting new work, let in-flight transactions
	// finish, then release the database. Jobs running on workers keep running;
	// their leases outlive this process and are reconciled on restart.
	log.Info("scheduler_shutting_down")
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)

	done := make(chan struct{})
	go func() { srv.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		srv.Stop()
	}
	log.Info("scheduler_stopped")
	return nil
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}

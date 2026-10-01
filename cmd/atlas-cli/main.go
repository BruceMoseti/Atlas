// Command atlas is the Atlas command-line client.
//
//	atlas submit --cpu 1 --memory 256m -- echo "hello atlas"
//	atlas get job_1a2b3c --watch
//	atlas list --state RUNNING
//	atlas cancel job_1a2b3c
//	atlas workers
//	atlas drain worker-3
//	atlas status
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/BruceMoseti/Atlas/internal/types"
	pb "github.com/BruceMoseti/Atlas/proto/atlaspb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const usage = `atlas - client for the Atlas distributed compute scheduler

Usage:
  atlas [--server ADDR] <command> [flags]

Commands:
  submit    submit a job
  get       show one job and its attempt history
  list      list jobs
  cancel    cancel a job
  workers   list the worker fleet
  drain     stop placing new work on a worker
  status    show cluster utilization and queue depth

Run 'atlas <command> -h' for command flags.
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "atlas:", err)
		os.Exit(1)
	}
}

func run() error {
	server := flag.String("server", envOr("ATLAS_SERVER", "localhost:50051"), "scheduler gRPC address")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("a command is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", *server, err)
	}
	defer conn.Close()
	client := pb.NewAtlasServiceClient(conn)

	switch args[0] {
	case "submit":
		// Not reordered: everything after `--` is the workload's own command
		// line and must be passed through untouched.
		return cmdSubmit(ctx, client, args[1:])
	case "get":
		return cmdGet(ctx, client, flagsFirst(args[1:]))
	case "list":
		return cmdList(ctx, client, flagsFirst(args[1:]))
	case "cancel":
		return cmdCancel(ctx, client, flagsFirst(args[1:]))
	case "workers":
		return cmdWorkers(ctx, client, flagsFirst(args[1:]))
	case "drain":
		return cmdDrain(ctx, client, flagsFirst(args[1:]))
	case "status":
		return cmdStatus(ctx, client)
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func cmdSubmit(ctx context.Context, c pb.AtlasServiceClient, argv []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	cpu := fs.String("cpu", "1", "CPU request in cores")
	memory := fs.String("memory", "256MB", "memory request, e.g. 512MB")
	image := fs.String("image", "", "container image; empty runs the command as a process on the worker")
	priority := fs.Int("priority", 0, "higher runs first")
	maxAttempts := fs.Int("max-attempts", 0, "attempt budget (0 uses the server default)")
	timeout := fs.Duration("timeout", 0, "kill the workload after this long (0 disables)")
	deadline := fs.Duration("deadline", 0, "relative deadline, used by the EDF queue ordering")
	key := fs.String("idempotency-key", "", "deduplicate submission; a repeat with the same key returns the same job")
	clientID := fs.String("client", "cli", "client id, used for per-client admission quotas")
	env := fs.String("env", "", "comma-separated KEY=VALUE pairs passed to the workload")
	retryExit := fs.Bool("retry-on-process-exit", false, "retry when the workload exits non-zero")
	retryTimeout := fs.Bool("retry-on-timeout", false, "retry when the workload times out")
	wait := fs.Bool("wait", false, "block until the job reaches a terminal state")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: atlas submit [flags] -- COMMAND [ARGS...]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(argv); err != nil {
		return err
	}

	command := fs.Args()
	if len(command) == 0 {
		return fmt.Errorf("a command is required: atlas submit [flags] -- echo hello")
	}

	cpuMillis, err := types.ParseCPUMillis(*cpu)
	if err != nil {
		return err
	}
	memBytes, err := types.ParseBytes(*memory)
	if err != nil {
		return err
	}
	envMap, err := parseKV(*env)
	if err != nil {
		return err
	}

	req := &pb.SubmitJobRequest{
		IdempotencyKey:     *key,
		ClientId:           *clientID,
		Image:              *image,
		Command:            command,
		Env:                envMap,
		CpuMillis:          cpuMillis,
		MemoryBytes:        memBytes,
		Priority:           int32(*priority),
		MaxAttempts:        int32(*maxAttempts),
		TimeoutSeconds:     int64(timeout.Seconds()),
		RetryOnProcessExit: *retryExit,
		RetryOnTimeout:     *retryTimeout,
	}
	if *deadline > 0 {
		req.DeadlineUnixNanos = time.Now().Add(*deadline).UnixNano()
	}

	res, err := c.SubmitJob(ctx, req)
	if err != nil {
		return err
	}
	if res.Deduplicated {
		fmt.Printf("%s\t%s\t(deduplicated: idempotency key already used)\n", res.JobId, stateName(res.State))
	} else {
		fmt.Printf("%s\t%s\n", res.JobId, stateName(res.State))
	}
	if !*wait {
		return nil
	}
	return watchJob(ctx, c, res.JobId)
}

func cmdGet(ctx context.Context, c pb.AtlasServiceClient, argv []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	watch := fs.Bool("watch", false, "follow the job until it reaches a terminal state")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: atlas get [--watch] JOB_ID")
	}
	if *watch {
		return watchJob(ctx, c, fs.Arg(0))
	}

	res, err := c.GetJob(ctx, &pb.GetJobRequest{JobId: fs.Arg(0), IncludeAttempts: true})
	if err != nil {
		return err
	}
	printJob(res.Job)
	return nil
}

func watchJob(ctx context.Context, c pb.AtlasServiceClient, jobID string) error {
	last := pb.JobState_JOB_STATE_UNSPECIFIED
	for {
		res, err := c.GetJob(ctx, &pb.GetJobRequest{JobId: jobID, IncludeAttempts: true})
		if err != nil {
			return err
		}
		if res.Job.State != last {
			fmt.Println(stateName(res.Job.State))
			last = res.Job.State
		}
		if terminal(res.Job.State) {
			fmt.Println()
			printJob(res.Job)
			if res.Job.State != pb.JobState_JOB_STATE_SUCCEEDED {
				os.Exit(1)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func cmdList(ctx context.Context, c pb.AtlasServiceClient, argv []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	states := fs.String("state", "", "comma-separated states to include, e.g. QUEUED,RUNNING")
	clientID := fs.String("client", "", "filter by client id")
	limit := fs.Int("limit", 50, "maximum jobs to return")
	if err := fs.Parse(argv); err != nil {
		return err
	}

	req := &pb.ListJobsRequest{ClientId: *clientID, Limit: int32(*limit)}
	for _, s := range splitNonEmpty(*states) {
		st, ok := parseJobState(s)
		if !ok {
			return fmt.Errorf("unknown job state %q", s)
		}
		req.States = append(req.States, st)
	}

	res, err := c.ListJobs(ctx, req)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB ID\tSTATE\tPRIO\tCPU\tMEMORY\tATTEMPTS\tAGE\tCOMMAND")
	for _, j := range res.Jobs {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%d/%d\t%s\t%s\n",
			j.JobId, stateName(j.State), j.Priority,
			types.FormatCPU(j.Request.CpuMillis), types.FormatBytes(j.Request.MemoryBytes),
			j.AttemptCount, j.MaxAttempts,
			age(j.CreatedAtUnixNanos), strings.Join(j.Command, " "))
	}
	return tw.Flush()
}

func cmdCancel(ctx context.Context, c pb.AtlasServiceClient, argv []string) error {
	fs := flag.NewFlagSet("cancel", flag.ExitOnError)
	reason := fs.String("reason", "canceled via CLI", "recorded with the cancellation")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: atlas cancel [--reason TEXT] JOB_ID")
	}
	res, err := c.CancelJob(ctx, &pb.CancelJobRequest{JobId: fs.Arg(0), Reason: *reason})
	if err != nil {
		return err
	}
	if res.Canceled {
		fmt.Printf("%s\tCANCELED\n", fs.Arg(0))
	} else {
		fmt.Printf("%s\t%s\t(already terminal; nothing to cancel)\n", fs.Arg(0), stateName(res.State))
	}
	return nil
}

func cmdWorkers(ctx context.Context, c pb.AtlasServiceClient, argv []string) error {
	fs := flag.NewFlagSet("workers", flag.ExitOnError)
	if err := fs.Parse(argv); err != nil {
		return err
	}
	res, err := c.ListWorkers(ctx, &pb.ListWorkersRequest{})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "WORKER ID\tSTATE\tCPU (alloc/cap)\tMEMORY (alloc/cap)\tRUNNING\tGEN\tLAST HEARTBEAT")
	for _, w := range res.Workers {
		fmt.Fprintf(tw, "%s\t%s\t%s/%s\t%s/%s\t%d\t%d\t%s ago\n",
			w.WorkerId, workerStateName(w.State),
			types.FormatCPU(w.Allocated.CpuMillis), types.FormatCPU(w.Capacity.CpuMillis),
			types.FormatBytes(w.Allocated.MemoryBytes), types.FormatBytes(w.Capacity.MemoryBytes),
			w.RunningAttempts, w.Generation, age(w.LastHeartbeatAtUnixNanos))
	}
	return tw.Flush()
}

func cmdDrain(ctx context.Context, c pb.AtlasServiceClient, argv []string) error {
	fs := flag.NewFlagSet("drain", flag.ExitOnError)
	undrain := fs.Bool("undo", false, "return a draining worker to service")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: atlas drain [--undo] WORKER_ID")
	}
	res, err := c.DrainWorker(ctx, &pb.DrainWorkerRequest{WorkerId: fs.Arg(0), Undrain: *undrain})
	if err != nil {
		return err
	}
	fmt.Printf("%s\t%s\n", fs.Arg(0), workerStateName(res.State))
	return nil
}

func cmdStatus(ctx context.Context, c pb.AtlasServiceClient) error {
	res, err := c.GetClusterStatus(ctx, &pb.ClusterStatusRequest{})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "scheduling policy\t%s\n", res.SchedulingPolicy)
	fmt.Fprintf(tw, "queue ordering\t%s\n", res.QueueOrdering)
	fmt.Fprintf(tw, "workers\t%d (%d healthy)\n", res.WorkersTotal, res.WorkersHealthy)
	fmt.Fprintf(tw, "cpu\t%s / %s (%.1f%%)\n",
		types.FormatCPU(res.Allocated.CpuMillis), types.FormatCPU(res.Capacity.CpuMillis), res.CpuUtilization*100)
	fmt.Fprintf(tw, "memory\t%s / %s (%.1f%%)\n",
		types.FormatBytes(res.Allocated.MemoryBytes), types.FormatBytes(res.Capacity.MemoryBytes), res.MemoryUtilization*100)
	fmt.Fprintf(tw, "jobs queued\t%d\n", res.JobsQueued)
	fmt.Fprintf(tw, "jobs running\t%d\n", res.JobsRunning)
	return tw.Flush()
}

func printJob(j *pb.Job) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "job\t%s\n", j.JobId)
	fmt.Fprintf(tw, "state\t%s\n", stateName(j.State))
	fmt.Fprintf(tw, "idempotency key\t%s\n", j.IdempotencyKey)
	fmt.Fprintf(tw, "client\t%s\n", j.ClientId)
	fmt.Fprintf(tw, "priority\t%d\n", j.Priority)
	fmt.Fprintf(tw, "request\t%s cpu, %s memory\n",
		types.FormatCPU(j.Request.CpuMillis), types.FormatBytes(j.Request.MemoryBytes))
	if j.Image != "" {
		fmt.Fprintf(tw, "image\t%s\n", j.Image)
	}
	fmt.Fprintf(tw, "command\t%s\n", strings.Join(j.Command, " "))
	fmt.Fprintf(tw, "attempts\t%d of %d\n", j.AttemptCount, j.MaxAttempts)
	if j.HasExitCode {
		fmt.Fprintf(tw, "exit code\t%d\n", j.ExitCode)
	}
	if j.FailureClass != pb.FailureClass_FAILURE_CLASS_UNSPECIFIED {
		fmt.Fprintf(tw, "failure class\t%s\n", failureName(j.FailureClass))
	}
	if j.Message != "" {
		fmt.Fprintf(tw, "message\t%s\n", j.Message)
	}
	fmt.Fprintf(tw, "created\t%s ago\n", age(j.CreatedAtUnixNanos))
	_ = tw.Flush()

	if len(j.Attempts) == 0 {
		return
	}
	fmt.Println()
	at := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(at, "#\tATTEMPT ID\tWORKER\tSTATE\tEXIT\tFAILURE\tDURATION\tMESSAGE")
	for _, a := range j.Attempts {
		exit := "-"
		if a.HasExitCode {
			exit = fmt.Sprint(a.ExitCode)
		}
		fmt.Fprintf(at, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			a.AttemptNumber, a.AttemptId, a.WorkerId, attemptStateName(a.State), exit,
			failureName(a.FailureClass), attemptDuration(a), a.Message)
	}
	_ = at.Flush()

	for _, a := range j.Attempts {
		if a.StdoutTail == "" && a.StderrTail == "" {
			continue
		}
		fmt.Printf("\n--- attempt %d (%s) output ---\n", a.AttemptNumber, a.AttemptId)
		if a.StdoutTail != "" {
			fmt.Print(a.StdoutTail)
			if !strings.HasSuffix(a.StdoutTail, "\n") {
				fmt.Println()
			}
		}
		if a.StderrTail != "" {
			fmt.Fprint(os.Stderr, a.StderrTail)
			if !strings.HasSuffix(a.StderrTail, "\n") {
				fmt.Fprintln(os.Stderr)
			}
		}
	}
}

func attemptDuration(a *pb.Attempt) string {
	if a.StartedAtUnixNanos == 0 {
		return "-"
	}
	end := a.FinishedAtUnixNanos
	if end == 0 {
		end = time.Now().UnixNano()
	}
	return time.Duration(end - a.StartedAtUnixNanos).Round(time.Millisecond).String()
}

func age(unixNanos int64) string {
	if unixNanos == 0 {
		return "-"
	}
	return time.Since(time.Unix(0, unixNanos)).Round(time.Millisecond).String()
}

func terminal(s pb.JobState) bool {
	switch s {
	case pb.JobState_JOB_STATE_SUCCEEDED, pb.JobState_JOB_STATE_FAILED, pb.JobState_JOB_STATE_CANCELED:
		return true
	}
	return false
}

func stateName(s pb.JobState) string {
	return strings.TrimPrefix(s.String(), "JOB_STATE_")
}

func attemptStateName(s pb.AttemptState) string {
	return strings.TrimPrefix(s.String(), "ATTEMPT_STATE_")
}

func workerStateName(s pb.WorkerState) string {
	return strings.TrimPrefix(s.String(), "WORKER_STATE_")
}

func failureName(c pb.FailureClass) string {
	if c == pb.FailureClass_FAILURE_CLASS_UNSPECIFIED {
		return "-"
	}
	return strings.TrimPrefix(c.String(), "FAILURE_CLASS_")
}

func parseJobState(s string) (pb.JobState, bool) {
	name := "JOB_STATE_" + strings.ToUpper(strings.TrimSpace(s))
	v, ok := pb.JobState_value[name]
	return pb.JobState(v), ok
}

func parseKV(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			return nil, fmt.Errorf("invalid env entry %q: want KEY=VALUE", pair)
		}
		out[k] = v
	}
	return out, nil
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// flagsFirst moves flags ahead of positional arguments.
//
// Go's flag package stops parsing at the first non-flag, so `atlas cancel JOB
// --reason x` would silently ignore --reason. Every other CLI a user has touched
// accepts that ordering, so Atlas does too rather than being surprising.
//
// A flag that takes a value is recognised by asking the command's own flag set,
// which is passed in by the caller as knownValueFlags.
func flagsFirst(args []string) []string {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "-") {
			positional = append(positional, a)
			continue
		}
		flags = append(flags, a)
		// `--flag value` needs its value kept adjacent. `--flag=value` and
		// boolean flags do not.
		if !strings.Contains(a, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			if valueFlags[strings.TrimLeft(a, "-")] {
				i++
				flags = append(flags, args[i])
			}
		}
	}
	return append(flags, positional...)
}

// valueFlags lists the flags on the reordered subcommands that take a value, so
// `--reason some text` keeps its argument. Boolean flags must not appear here or
// they would swallow the positional that follows them.
var valueFlags = map[string]bool{
	"reason": true,
	"state":  true,
	"client": true,
	"limit":  true,
}

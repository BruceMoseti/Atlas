package chaos

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"
)

// WriteReport renders the campaign result.
//
// The format is deliberately blunt. Every line is either a count of something that
// happened or a count of something that was checked, and the numbers that would be
// embarrassing — duplicate executions, jobs abandoned after exhausting their
// retries — are printed in the same place as the flattering ones. A chaos report
// that only shows what went right is marketing.
func WriteReport(w io.Writer, r *Report) {
	fmt.Fprintf(w, "Atlas Chaos Report\n")
	fmt.Fprintf(w, "==================\n\n")

	host, _ := os.Hostname()
	fmt.Fprintf(w, "Environment\n")
	fmt.Fprintf(w, "  host                      %s\n", host)
	fmt.Fprintf(w, "  go / os / arch            %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(w, "  cpus                      %d\n", runtime.NumCPU())
	fmt.Fprintf(w, "  started                   %s\n", r.Started.UTC().Format(time.RFC3339))
	fmt.Fprintf(w, "  wall clock                %s\n\n", r.Finished.Sub(r.Started).Round(time.Second))

	fmt.Fprintf(w, "Configuration\n")
	fmt.Fprintf(w, "  workers                   %d\n", r.Config.Workers)
	fmt.Fprintf(w, "  jobs                      %d\n", r.Config.Jobs)
	fmt.Fprintf(w, "  job duration              %s\n", r.Config.JobDuration)
	fmt.Fprintf(w, "  attempt budget            %d\n", r.Config.MaxAttempts)
	fmt.Fprintf(w, "  fault window              %s (mean interval %s)\n", r.Config.Duration, r.Config.FaultInterval)
	fmt.Fprintf(w, "  lease ttl                 %s\n", r.Config.LeaseTTL)
	fmt.Fprintf(w, "  heartbeat suspect / dead  %s / %s\n", r.Config.SuspectAfter, r.Config.DeadAfter)
	fmt.Fprintf(w, "  duplicate rpc rate        %.2f\n", r.Config.DuplicateRPCRate)
	fmt.Fprintf(w, "  heartbeat drop rate       %.2f\n", r.Config.HeartbeatDropRate)
	fmt.Fprintf(w, "  lease renewal drop rate   %.2f\n", r.Config.RenewDropRate)
	fmt.Fprintf(w, "  seed                      %d\n\n", r.Config.Seed)

	fmt.Fprintf(w, "Faults injected\n")
	if len(r.Faults) == 0 {
		fmt.Fprintf(w, "  (none at the process level)\n")
	}
	kinds := make([]string, 0, len(r.FaultsByKind))
	for k := range r.FaultsByKind {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		fmt.Fprintf(w, "  %-25s %d\n", k, r.FaultsByKind[k])
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "Submission\n")
	fmt.Fprintf(w, "  submitted                 %d\n", r.JobsSubmitted)
	fmt.Fprintf(w, "  accepted                  %d\n", r.JobsAccepted)
	fmt.Fprintf(w, "  rejected (admission)      %d\n", r.JobsRejected)
	fmt.Fprintf(w, "  failed to submit          %d\n\n", r.SubmitFailures)

	if ir := r.InvariantsRep; ir != nil {
		fmt.Fprintf(w, "Outcome\n")
		fmt.Fprintf(w, "  jobs in store             %d\n", ir.JobsTotal)
		fmt.Fprintf(w, "  terminal                  %d\n", ir.JobsTerminal)
		fmt.Fprintf(w, "  still pending             %d\n", ir.JobsPending)
		fmt.Fprintf(w, "  succeeded                 %d\n", ir.JobsSucceeded)
		fmt.Fprintf(w, "  failed after retry limit  %d\n", ir.JobsFailed)
		fmt.Fprintf(w, "  canceled                  %d\n\n", ir.JobsCanceled)

		fmt.Fprintf(w, "Execution attempts\n")
		fmt.Fprintf(w, "  attempts total            %d\n", ir.AttemptsTotal)
		fmt.Fprintf(w, "  succeeded                 %d\n", ir.AttemptsSucceeded)
		fmt.Fprintf(w, "  failed                    %d\n", ir.AttemptsFailed)
		fmt.Fprintf(w, "  lost (lease reclaimed)    %d\n", ir.AttemptsLost)
		fmt.Fprintf(w, "  canceled                  %d\n", ir.AttemptsCanceled)
		fmt.Fprintf(w, "  retries                   %d\n", ir.Retries)
		fmt.Fprintf(w, "  most attempts on one job  %d\n", ir.MaxAttemptsUsed)
		fmt.Fprintf(w, "  duplicate executions      %d  (reclaimed attempts that later\n", ir.DuplicateExecutions)
		fmt.Fprintf(w, "                                reported success: work that really did\n")
		fmt.Fprintf(w, "                                run twice, which at-least-once allows)\n\n")

		fmt.Fprintf(w, "Recovery latency (lease reclaimed to replacement attempt assigned)\n")
		fmt.Fprintf(w, "  p50                       %s\n", r.RecoveryP50.Round(time.Millisecond))
		fmt.Fprintf(w, "  p99                       %s\n", r.RecoveryP99.Round(time.Millisecond))
		fmt.Fprintf(w, "  max                       %s\n", r.RecoveryMax.Round(time.Millisecond))
		fmt.Fprintf(w, "  (excludes detection, which is bounded above by the %s dead-after\n", r.Config.DeadAfter)
		fmt.Fprintf(w, "   threshold for a worker that stops heartbeating, and by the %s lease\n", r.Config.LeaseTTL)
		fmt.Fprintf(w, "   TTL for one that is alive but stops renewing)\n\n")

		fmt.Fprintf(w, "Invariants (docs/SEMANTICS.md §7), checked against %d audited state changes\n", ir.Transitions)
		broken := map[string]int{}
		for _, v := range ir.Violations {
			broken[v.Invariant]++
		}
		for _, inv := range invariantList {
			if n := broken[inv.id]; n > 0 {
				fmt.Fprintf(w, "  %-3s %-52s VIOLATED (%d)\n", inv.id, inv.desc, n)
				continue
			}
			fmt.Fprintf(w, "  %-3s %-52s held\n", inv.id, inv.desc)
		}
		if len(ir.Violations) > 0 {
			fmt.Fprintf(w, "\n  violations in detail:\n")
			for _, v := range ir.Violations {
				fmt.Fprintf(w, "    %s\n", v)
			}
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "Drained within timeout      %v\n", r.Drained)
	fmt.Fprintf(w, "\nVERDICT: %s\n", verdict(r))

	fmt.Fprintf(w, "\nWhat this does and does not show\n")
	fmt.Fprintf(w, "  Every number above comes from the SQLite database the run produced,\n")
	fmt.Fprintf(w, "  read after every process had exited. The attempt counts are physical\n")
	fmt.Fprintf(w, "  executions, not logical jobs: under at-least-once semantics a job that\n")
	fmt.Fprintf(w, "  was retried really did run more than once, and Atlas does not pretend\n")
	fmt.Fprintf(w, "  otherwise. What it does claim is that no accepted job disappeared, no\n")
	fmt.Fprintf(w, "  terminal state was ever revisited, no worker was oversubscribed, and no\n")
	fmt.Fprintf(w, "  stale attempt decided a job's outcome.\n")
}

// invariantList mirrors docs/SEMANTICS.md §7 so the report names what it checked,
// not merely how many checks failed. A report that says "0 violations" without
// saying of what is not evidence of anything.
var invariantList = []struct{ id, desc string }{
	{"I1", "terminal states were never left"},
	{"I2", "no worker was oversubscribed or went negative"},
	{"I3", "allocation equals the sum of live attempts"},
	{"I4", "at most one live attempt per job"},
	{"I5", "attempt counts stayed within budget"},
	{"I6", "every accepted job is still in the store"},
	{"I7", "idempotency keys are unique"},
	{"I8", "no stale attempt decided a job's outcome"},
	{"I9", "job and attempt states agree"},
}

func verdict(r *Report) string {
	var problems []string
	if r.InvariantsRep == nil {
		return "INCOMPLETE - the invariant check did not run"
	}
	if n := len(r.InvariantsRep.Violations); n > 0 {
		problems = append(problems, fmt.Sprintf("%d invariant violations", n))
	}
	if !r.Drained {
		problems = append(problems, "cluster did not drain within the timeout")
	}
	if r.SubmitFailures > 0 {
		problems = append(problems, fmt.Sprintf("%d submissions never got an answer", r.SubmitFailures))
	}
	if len(problems) == 0 {
		return "PASS - every documented invariant held under fault injection"
	}
	return "FAIL - " + strings.Join(problems, "; ")
}

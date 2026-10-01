// Package invariants mechanically checks the properties Atlas claims in
// docs/SEMANTICS.md §7.
//
// This is the difference between "I added fault tolerance" and "here are the
// properties, here is the checker, and here is the run where it found nothing".
// Every chaos campaign and every integration test ends by running Check against the
// database the run actually produced.
//
// The checker reads only persisted state: the jobs, attempts, and workers tables
// plus the append-only transitions log. It therefore validates the system's own
// durable record rather than some in-memory bookkeeping that could be wrong in the
// same way the scheduler is.
package invariants

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/store"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// Violation is one broken invariant.
type Violation struct {
	// Invariant is the identifier from docs/SEMANTICS.md, e.g. "I2".
	Invariant string
	Subject   string
	Detail    string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s [%s] %s", v.Invariant, v.Subject, v.Detail)
}

// Report is the outcome of a check.
type Report struct {
	Violations []Violation

	JobsTotal     int
	JobsTerminal  int
	JobsSucceeded int
	JobsFailed    int
	JobsCanceled  int
	JobsPending   int

	AttemptsTotal     int
	AttemptsSucceeded int
	AttemptsFailed    int
	AttemptsLost      int
	AttemptsCanceled  int
	AttemptsLive      int

	// Retries counts attempts beyond the first, i.e. physical executions caused
	// by a retry.
	Retries int
	// DuplicateExecutions counts reclaimed attempts that later reported
	// success. Each one is a logical job that definitely ran more than once,
	// which at-least-once semantics permits and Atlas therefore measures
	// rather than denies.
	DuplicateExecutions int
	// MaxAttemptsUsed is the largest attempt count on any single job.
	MaxAttemptsUsed int32

	WorkersTotal int
	Transitions  int
}

// OK reports whether every invariant held.
func (r *Report) OK() bool { return len(r.Violations) == 0 }

func (r *Report) add(invariant, subject, format string, args ...any) {
	r.Violations = append(r.Violations, Violation{
		Invariant: invariant,
		Subject:   subject,
		Detail:    fmt.Sprintf(format, args...),
	})
}

// String renders the report the way the chaos harness prints it.
func (r *Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "jobs                  %d (%d terminal, %d pending)\n", r.JobsTotal, r.JobsTerminal, r.JobsPending)
	fmt.Fprintf(&b, "  succeeded           %d\n", r.JobsSucceeded)
	fmt.Fprintf(&b, "  failed              %d\n", r.JobsFailed)
	fmt.Fprintf(&b, "  canceled            %d\n", r.JobsCanceled)
	fmt.Fprintf(&b, "attempts              %d (%d live)\n", r.AttemptsTotal, r.AttemptsLive)
	fmt.Fprintf(&b, "  succeeded           %d\n", r.AttemptsSucceeded)
	fmt.Fprintf(&b, "  failed              %d\n", r.AttemptsFailed)
	fmt.Fprintf(&b, "  lost                %d\n", r.AttemptsLost)
	fmt.Fprintf(&b, "  canceled            %d\n", r.AttemptsCanceled)
	fmt.Fprintf(&b, "retries               %d (max attempts used on one job: %d)\n", r.Retries, r.MaxAttemptsUsed)
	fmt.Fprintf(&b, "duplicate executions  %d\n", r.DuplicateExecutions)
	fmt.Fprintf(&b, "workers               %d\n", r.WorkersTotal)
	fmt.Fprintf(&b, "recorded transitions  %d\n", r.Transitions)
	fmt.Fprintf(&b, "invariant violations  %d\n", len(r.Violations))
	for _, v := range r.Violations {
		fmt.Fprintf(&b, "  %s\n", v)
	}
	return b.String()
}

// Check validates every invariant against the store and returns a report.
//
// It never returns an error for a violated invariant: a violation is data, and the
// caller decides whether it is fatal. An error means the check itself could not run.
func Check(ctx context.Context, st *store.Store) (*Report, error) {
	r := &Report{}

	var (
		jobs        []*types.Job
		attempts    []*types.Attempt
		workers     []*types.Worker
		transitions []types.Transition
	)
	err := st.View(ctx, func(tx *store.Tx) error {
		var err error
		if jobs, err = tx.ListJobs(store.JobFilter{}); err != nil {
			return err
		}
		if workers, err = tx.ListWorkers(); err != nil {
			return err
		}
		if transitions, err = tx.Transitions(""); err != nil {
			return err
		}
		for _, j := range jobs {
			as, err := tx.ListAttemptsForJob(j.ID)
			if err != nil {
				return err
			}
			attempts = append(attempts, as...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("invariants: read state: %w", err)
	}

	byJob := make(map[string][]*types.Attempt, len(jobs))
	for _, a := range attempts {
		byJob[a.JobID] = append(byJob[a.JobID], a)
	}
	for _, as := range byJob {
		sort.Slice(as, func(i, j int) bool { return as[i].Number < as[j].Number })
	}

	r.WorkersTotal = len(workers)
	r.Transitions = len(transitions)

	checkTerminalStatesAreAbsorbing(r, transitions)
	checkIdempotencyKeysAreUnique(r, jobs)
	checkJobs(r, jobs, byJob)
	checkAttempts(r, attempts)
	checkWorkerCapacity(r, workers, attempts)

	return r, nil
}

// I1: no job or attempt ever transitioned out of a terminal state.
//
// The audit log is what makes this provable over the whole run. Checking only the
// final rows would miss a job that went SUCCEEDED, then RUNNING, then SUCCEEDED
// again — which is exactly the bug a stale worker report would cause.
func checkTerminalStatesAreAbsorbing(r *Report, transitions []types.Transition) {
	for _, t := range transitions {
		var terminal bool
		switch t.Kind {
		case "job":
			terminal = state.JobState(t.FromState).IsTerminal()
		case "attempt":
			terminal = state.AttemptState(t.FromState).IsTerminal()
		}
		if terminal {
			subject := t.JobID
			if t.Kind == "attempt" {
				subject = t.AttemptID
			}
			r.add("I1", subject, "%s left terminal state %s for %s (seq %d, reason %q)",
				t.Kind, t.FromState, t.ToState, t.Seq, t.Reason)
		}
	}
}

// I7: no two jobs share an idempotency key.
func checkIdempotencyKeysAreUnique(r *Report, jobs []*types.Job) {
	seen := make(map[string]string, len(jobs))
	for _, j := range jobs {
		if j.IdempotencyKey == "" {
			r.add("I7", j.ID, "job has an empty idempotency key")
			continue
		}
		if other, dup := seen[j.IdempotencyKey]; dup {
			r.add("I7", j.ID, "shares idempotency key %q with job %s", j.IdempotencyKey, other)
			continue
		}
		seen[j.IdempotencyKey] = j.ID
	}
}

// checkJobs covers I4 (one live attempt), I5 (bounded retries), I8 (no stale write
// decided the outcome), and I9 (job and attempt states agree).
func checkJobs(r *Report, jobs []*types.Job, byJob map[string][]*types.Attempt) {
	for _, j := range jobs {
		r.JobsTotal++
		switch j.State {
		case state.JobSucceeded:
			r.JobsTerminal++
			r.JobsSucceeded++
		case state.JobFailed:
			r.JobsTerminal++
			r.JobsFailed++
		case state.JobCanceled:
			r.JobsTerminal++
			r.JobsCanceled++
		default:
			r.JobsPending++
		}
		if j.AttemptCount > r.MaxAttemptsUsed {
			r.MaxAttemptsUsed = j.AttemptCount
		}
		if j.AttemptCount > 1 {
			r.Retries += int(j.AttemptCount) - 1
		}

		as := byJob[j.ID]

		// I5: the attempt budget is real, and the attempt rows agree with the
		// counter the scheduler maintains.
		if j.AttemptCount > j.MaxAttempts {
			r.add("I5", j.ID, "attempt_count %d exceeds max_attempts %d", j.AttemptCount, j.MaxAttempts)
		}
		if int(j.AttemptCount) != len(as) {
			r.add("I5", j.ID, "attempt_count is %d but %d attempt rows exist", j.AttemptCount, len(as))
		}
		for i, a := range as {
			if int(a.Number) != i+1 {
				r.add("I5", j.ID, "attempt numbers are not contiguous: position %d holds attempt %d", i+1, a.Number)
			}
		}

		// I4: at most one attempt is live, and if one is, the job points at it.
		var live []*types.Attempt
		for _, a := range as {
			if !a.State.IsTerminal() {
				live = append(live, a)
			}
		}
		if len(live) > 1 {
			ids := make([]string, len(live))
			for i, a := range live {
				ids[i] = a.ID
			}
			r.add("I4", j.ID, "%d attempts are live at once: %s", len(live), strings.Join(ids, ", "))
		}
		if len(live) == 1 && j.CurrentAttemptID != live[0].ID {
			r.add("I4", j.ID, "live attempt %s is not the job's current_attempt_id (%q)", live[0].ID, j.CurrentAttemptID)
		}
		if j.State.IsTerminal() && len(live) > 0 {
			r.add("I4", j.ID, "job is %s but attempt %s is still live", j.State, live[0].ID)
		}

		// I8: the outcome reflected in the job came from its last attempt, not
		// from a reclaimed one that reported late.
		if len(as) > 0 && (j.State == state.JobSucceeded || j.State == state.JobFailed) {
			last := as[len(as)-1]
			switch j.State {
			case state.JobSucceeded:
				if last.State != state.AttemptSucceeded {
					r.add("I8", j.ID, "job SUCCEEDED but its last attempt %s is %s", last.ID, last.State)
				}
				for _, a := range as[:len(as)-1] {
					if a.State == state.AttemptSucceeded {
						r.add("I8", j.ID, "earlier attempt %s also SUCCEEDED; the outcome may have come from a stale write", a.ID)
					}
				}
			case state.JobFailed:
				if last.State == state.AttemptSucceeded {
					r.add("I8", j.ID, "job FAILED but its last attempt %s SUCCEEDED", last.ID)
				}
			}
		}

		// I9: job state and current attempt state are consistent.
		switch j.State {
		case state.JobRunning:
			cur := findAttempt(as, j.CurrentAttemptID)
			if cur == nil {
				r.add("I9", j.ID, "job is RUNNING with no current attempt")
			} else if cur.State != state.AttemptRunning && cur.State != state.AttemptAssigned {
				r.add("I9", j.ID, "job is RUNNING but attempt %s is %s", cur.ID, cur.State)
			}
		case state.JobAssigned:
			cur := findAttempt(as, j.CurrentAttemptID)
			if cur == nil {
				r.add("I9", j.ID, "job is ASSIGNED with no current attempt")
			} else if cur.State.IsTerminal() {
				r.add("I9", j.ID, "job is ASSIGNED but attempt %s is already %s", cur.ID, cur.State)
			}
		case state.JobSucceeded:
			cur := findAttempt(as, j.CurrentAttemptID)
			if cur == nil {
				r.add("I9", j.ID, "job SUCCEEDED with no current attempt recorded")
			} else if cur.State != state.AttemptSucceeded {
				r.add("I9", j.ID, "job SUCCEEDED but its current attempt %s is %s", cur.ID, cur.State)
			}
		case state.JobQueued:
			if j.CurrentAttemptID != "" {
				if cur := findAttempt(as, j.CurrentAttemptID); cur != nil && !cur.State.IsTerminal() {
					r.add("I9", j.ID, "job is QUEUED but still owns live attempt %s", cur.ID)
				}
			}
		}
	}
}

// I5 and the attempt tallies.
func checkAttempts(r *Report, attempts []*types.Attempt) {
	for _, a := range attempts {
		r.AttemptsTotal++
		switch a.State {
		case state.AttemptSucceeded:
			r.AttemptsSucceeded++
		case state.AttemptFailed:
			r.AttemptsFailed++
		case state.AttemptLost:
			r.AttemptsLost++
		case state.AttemptCanceled:
			r.AttemptsCanceled++
		default:
			r.AttemptsLive++
		}
		if a.State == state.AttemptLost && strings.Contains(a.Message, state.LateReportMarker) {
			r.DuplicateExecutions++
		}
		if a.LeaseID == "" {
			r.add("I3", a.ID, "attempt has no lease id")
		}
		if a.State.IsTerminal() && a.FinishedAt == nil {
			r.add("I9", a.ID, "attempt is %s but has no finish time", a.State)
		}
	}
}

// I2 and I3: allocations are within capacity, non-negative, and exactly equal to the
// sum of the live attempts on the worker.
//
// I3 is why the denormalized allocation columns are trustworthy. They exist so that
// placement does not need an aggregate query; this check is what keeps them honest.
func checkWorkerCapacity(r *Report, workers []*types.Worker, attempts []*types.Attempt) {
	derived := make(map[string]types.Resources, len(workers))
	for _, a := range attempts {
		if !a.State.IsTerminal() {
			derived[a.WorkerID] = derived[a.WorkerID].Add(a.Request)
		}
	}

	known := make(map[string]bool, len(workers))
	for _, w := range workers {
		known[w.ID] = true

		if w.Allocated.CPUMillis < 0 || w.Allocated.MemoryBytes < 0 {
			r.add("I2", w.ID, "allocation is negative: cpu=%d mem=%d",
				w.Allocated.CPUMillis, w.Allocated.MemoryBytes)
		}
		if !w.Allocated.Fits(w.Capacity) {
			r.add("I2", w.ID, "allocated cpu=%d/%d mem=%d/%d exceeds capacity",
				w.Allocated.CPUMillis, w.Capacity.CPUMillis,
				w.Allocated.MemoryBytes, w.Capacity.MemoryBytes)
		}
		if got, want := w.Allocated, derived[w.ID]; got != want {
			r.add("I3", w.ID, "allocation is cpu=%d mem=%d but live attempts sum to cpu=%d mem=%d",
				got.CPUMillis, got.MemoryBytes, want.CPUMillis, want.MemoryBytes)
		}
	}
	for id, res := range derived {
		if !known[id] && (res.CPUMillis > 0 || res.MemoryBytes > 0) {
			r.add("I3", id, "live attempts reference a worker with no row (cpu=%d mem=%d)",
				res.CPUMillis, res.MemoryBytes)
		}
	}
}

// CheckAcceptedJobs verifies invariant I6 against a list of job ids a client was
// told were accepted. Every one of them must still exist.
//
// It is separate from Check because only the client knows what it submitted; the
// database cannot tell you about a job it lost.
func CheckAcceptedJobs(ctx context.Context, st *store.Store, acceptedJobIDs []string) ([]Violation, error) {
	var out []Violation
	err := st.View(ctx, func(tx *store.Tx) error {
		for _, id := range acceptedJobIDs {
			if _, err := tx.GetJob(id); err != nil {
				out = append(out, Violation{
					Invariant: "I6",
					Subject:   id,
					Detail:    "job was acknowledged to a client but is not in the store: " + err.Error(),
				})
			}
		}
		return nil
	})
	return out, err
}

func findAttempt(attempts []*types.Attempt, id string) *types.Attempt {
	if id == "" {
		return nil
	}
	for _, a := range attempts {
		if a.ID == id {
			return a
		}
	}
	return nil
}

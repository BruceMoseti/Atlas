// Package types holds the domain objects shared by the store, scheduler, API, and
// invariant checker. It deliberately contains no behaviour beyond trivial accessors,
// so that it can be imported from anywhere without creating cycles.
package types

import (
	"time"

	"github.com/BruceMoseti/Atlas/internal/state"
)

// Resources is a resource request or a capacity. CPU is in millicores (1000 = one
// core) so that fractional requests need no floating point in the accounting path.
type Resources struct {
	CPUMillis   int64
	MemoryBytes int64
}

// Add returns r+o.
func (r Resources) Add(o Resources) Resources {
	return Resources{CPUMillis: r.CPUMillis + o.CPUMillis, MemoryBytes: r.MemoryBytes + o.MemoryBytes}
}

// Sub returns r-o. It may go negative; the caller is responsible for checking, and
// the invariant checker asserts that persisted allocations never do.
func (r Resources) Sub(o Resources) Resources {
	return Resources{CPUMillis: r.CPUMillis - o.CPUMillis, MemoryBytes: r.MemoryBytes - o.MemoryBytes}
}

// Fits reports whether a request of size r can be satisfied by capacity c.
func (r Resources) Fits(c Resources) bool {
	return r.CPUMillis <= c.CPUMillis && r.MemoryBytes <= c.MemoryBytes
}

// IsZero reports whether the request is empty.
func (r Resources) IsZero() bool { return r.CPUMillis == 0 && r.MemoryBytes == 0 }

// Job is a logical unit of work. It may be executed more than once; see
// docs/SEMANTICS.md.
type Job struct {
	ID             string
	IdempotencyKey string
	ClientID       string

	State    state.JobState
	Priority int32

	Request Resources

	Image   string
	Command []string
	Env     map[string]string

	MaxAttempts  int32
	AttemptCount int32
	Timeout      time.Duration

	RetryOnProcessExit bool
	RetryOnTimeout     bool

	CreatedAt time.Time
	UpdatedAt time.Time
	// EnqueuedAt is when the job most recently entered QUEUED. Priority aging is
	// measured from this point, and it survives scheduler restart so that a
	// restart does not reset a job's accrued urgency.
	EnqueuedAt time.Time
	// EligibleAt gates retry backoff: the job stays QUEUED but is not dispatched
	// until this time. Equal to EnqueuedAt when there is no backoff.
	EligibleAt time.Time
	// Deadline is optional and is only consulted by the EDF ordering.
	Deadline *time.Time

	CurrentAttemptID string

	// Terminal outcome, populated when State is terminal.
	ExitCode     *int32
	FailureClass state.FailureClass
	Message      string

	// RequestSpecHash detects idempotency-key reuse with different parameters.
	RequestSpecHash string
}

// Retry returns the retry policy implied by the job's flags.
func (j *Job) Retry() state.RetryPolicy {
	return state.RetryPolicy{RetryOnProcessExit: j.RetryOnProcessExit, RetryOnTimeout: j.RetryOnTimeout}
}

// AttemptsRemaining reports how many more attempts the budget allows.
func (j *Job) AttemptsRemaining() int32 {
	if j.AttemptCount >= j.MaxAttempts {
		return 0
	}
	return j.MaxAttempts - j.AttemptCount
}

// Attempt is one physical execution of a job on one worker, scoped by one lease.
type Attempt struct {
	ID       string
	JobID    string
	Number   int32
	WorkerID string

	State state.AttemptState

	LeaseID        string
	LeaseExpiresAt time.Time

	// Request is copied from the job so that resource accounting for a live
	// attempt never depends on the job row, which may change.
	Request Resources

	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time

	ExitCode     *int32
	FailureClass state.FailureClass
	Message      string
	StdoutTail   string
	StderrTail   string
}

// Worker is a member of the fleet as the scheduler believes it to be.
type Worker struct {
	ID       string
	Hostname string
	Version  string
	Labels   map[string]string

	Capacity  Resources
	Allocated Resources

	State           state.WorkerState
	RegisteredAt    time.Time
	LastHeartbeatAt time.Time
	// Generation increments on every (re-)registration. A worker that restarts
	// with the same ID gets a new generation, which lets the scheduler reclaim
	// the leases the previous incarnation held.
	Generation int64
}

// Available returns capacity minus allocation.
func (w *Worker) Available() Resources { return w.Capacity.Sub(w.Allocated) }

// Transition is one row of the append-only audit log of job state changes. The log
// exists so the invariant checker can prove that terminal states were absorbing over
// the whole history, not merely at the end.
type Transition struct {
	Seq       int64
	JobID     string
	AttemptID string
	FromState string
	ToState   string
	Kind      string // "job" or "attempt"
	Reason    string
	At        time.Time
}

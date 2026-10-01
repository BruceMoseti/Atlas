// Package state defines Atlas's job and attempt state machines and the only
// sanctioned way to move between states.
//
// Nothing outside this package may assign a state directly. Every transition goes
// through Transition or TransitionAttempt, which reject illegal moves. Centralizing
// this is what makes invariant I1 ("terminal states are absorbing") checkable by
// construction rather than by inspection.
package state

import "fmt"

// JobState is the state of a logical job.
type JobState string

const (
	// JobSubmitted exists only inside the submission transaction: the row is
	// written, validated into JobQueued, and committed. It is never observable.
	JobSubmitted JobState = "SUBMITTED"
	JobQueued    JobState = "QUEUED"
	JobAssigned  JobState = "ASSIGNED"
	JobRunning   JobState = "RUNNING"
	JobSucceeded JobState = "SUCCEEDED"
	JobFailed    JobState = "FAILED"
	JobCanceled  JobState = "CANCELED"
)

// AttemptState is the state of one physical execution of a job.
type AttemptState string

const (
	AttemptAssigned  AttemptState = "ASSIGNED"
	AttemptRunning   AttemptState = "RUNNING"
	AttemptSucceeded AttemptState = "SUCCEEDED"
	AttemptFailed    AttemptState = "FAILED"
	// AttemptLost means Atlas does not know what happened: the lease expired or
	// the worker was declared dead. It is distinct from AttemptFailed, which
	// means Atlas knows the execution finished badly.
	AttemptLost     AttemptState = "LOST"
	AttemptCanceled AttemptState = "CANCELED"
)

var jobTransitions = map[JobState]map[JobState]bool{
	JobSubmitted: {JobQueued: true, JobCanceled: true},
	JobQueued:    {JobAssigned: true, JobCanceled: true, JobFailed: true},
	JobAssigned:  {JobRunning: true, JobQueued: true, JobCanceled: true, JobFailed: true},
	JobRunning:   {JobSucceeded: true, JobFailed: true, JobCanceled: true, JobQueued: true},
	JobSucceeded: {},
	JobFailed:    {},
	JobCanceled:  {},
}

var attemptTransitions = map[AttemptState]map[AttemptState]bool{
	AttemptAssigned:  {AttemptRunning: true, AttemptFailed: true, AttemptLost: true, AttemptCanceled: true},
	AttemptRunning:   {AttemptSucceeded: true, AttemptFailed: true, AttemptLost: true, AttemptCanceled: true},
	AttemptSucceeded: {},
	AttemptFailed:    {},
	AttemptLost:      {},
	AttemptCanceled:  {},
}

// IsTerminal reports whether a job state is absorbing.
func (s JobState) IsTerminal() bool {
	return s == JobSucceeded || s == JobFailed || s == JobCanceled
}

// IsTerminal reports whether an attempt state is absorbing.
func (s AttemptState) IsTerminal() bool {
	switch s {
	case AttemptSucceeded, AttemptFailed, AttemptLost, AttemptCanceled:
		return true
	}
	return false
}

// Valid reports whether s is a state this version of Atlas knows about.
func (s JobState) Valid() bool {
	_, ok := jobTransitions[s]
	return ok
}

// Valid reports whether s is a state this version of Atlas knows about.
func (s AttemptState) Valid() bool {
	_, ok := attemptTransitions[s]
	return ok
}

// ErrIllegalTransition describes a rejected state change. It carries both states so
// that callers can log a useful message and tests can assert on the specific edge.
type ErrIllegalTransition struct {
	Kind string
	From string
	To   string
}

func (e *ErrIllegalTransition) Error() string {
	return fmt.Sprintf("illegal %s transition %s -> %s", e.Kind, e.From, e.To)
}

// Transition validates a job state change. It returns the new state on success so
// that callers can write `job.State, err = state.Transition(job.State, next)` and
// never have an unvalidated assignment in their code.
func Transition(from, to JobState) (JobState, error) {
	allowed, known := jobTransitions[from]
	if !known {
		return from, &ErrIllegalTransition{Kind: "job", From: string(from), To: string(to)}
	}
	if !allowed[to] {
		return from, &ErrIllegalTransition{Kind: "job", From: string(from), To: string(to)}
	}
	return to, nil
}

// TransitionAttempt validates an attempt state change.
func TransitionAttempt(from, to AttemptState) (AttemptState, error) {
	allowed, known := attemptTransitions[from]
	if !known {
		return from, &ErrIllegalTransition{Kind: "attempt", From: string(from), To: string(to)}
	}
	if !allowed[to] {
		return from, &ErrIllegalTransition{Kind: "attempt", From: string(from), To: string(to)}
	}
	return to, nil
}

// CanTransition reports whether a job edge is legal, without constructing an error.
func CanTransition(from, to JobState) bool {
	return jobTransitions[from][to]
}

// CanTransitionAttempt reports whether an attempt edge is legal.
func CanTransitionAttempt(from, to AttemptState) bool {
	return attemptTransitions[from][to]
}

// AllJobStates returns every job state, for exhaustive tests and the invariant checker.
func AllJobStates() []JobState {
	return []JobState{JobSubmitted, JobQueued, JobAssigned, JobRunning, JobSucceeded, JobFailed, JobCanceled}
}

// AllAttemptStates returns every attempt state.
func AllAttemptStates() []AttemptState {
	return []AttemptState{AttemptAssigned, AttemptRunning, AttemptSucceeded, AttemptFailed, AttemptLost, AttemptCanceled}
}

// WorkerState is the scheduler's belief about a worker, derived from heartbeats and
// operator actions. It is a belief, not a fact: a SUSPECT worker may be perfectly
// healthy but partitioned.
type WorkerState string

const (
	WorkerHealthy WorkerState = "HEALTHY"
	// WorkerSuspect means one or more heartbeats have been missed. The worker keeps
	// its existing assignments but receives no new ones.
	WorkerSuspect  WorkerState = "SUSPECT"
	WorkerDead     WorkerState = "DEAD"
	WorkerDraining WorkerState = "DRAINING"
	WorkerDrained  WorkerState = "DRAINED"
)

// Schedulable reports whether the scheduler may place new work on a worker in this
// state. Only HEALTHY qualifies: SUSPECT workers are excluded precisely because we
// are unsure about them, and draining workers are excluded by operator intent.
func (s WorkerState) Schedulable() bool { return s == WorkerHealthy }

// LateReportMarker is appended to a reclaimed attempt's message when the worker
// that held it reports success after the fact.
//
// That is a directly observed duplicate physical execution: Atlas requeued a job
// whose earlier execution had in fact completed. The marker makes the count
// recoverable from the durable record alone, so the chaos report can publish it
// even across a scheduler restart that reset the in-memory counters.
const LateReportMarker = "late-success-after-reclaim"

// FailureClass categorizes why an attempt ended badly. The class drives the retry
// decision, because "the program exited 1" and "we lost contact with the machine"
// deserve different treatment.
type FailureClass string

const (
	FailureNone          FailureClass = ""
	FailureProcessExit   FailureClass = "PROCESS_EXIT"
	FailureTimeout       FailureClass = "TIMEOUT"
	FailureWorkerLost    FailureClass = "WORKER_LOST"
	FailureResourceError FailureClass = "RESOURCE_ERROR"
	FailureSystemError   FailureClass = "SYSTEM_ERROR"
	FailureCanceled      FailureClass = "CANCELED"
)

// RetryPolicy says which failure classes are worth another attempt.
type RetryPolicy struct {
	// RetryOnProcessExit retries a workload that ran to completion and exited
	// non-zero. Off by default: a deterministic program will produce the same
	// exit code again, and retrying it just burns capacity.
	RetryOnProcessExit bool
	// RetryOnTimeout retries a workload that exceeded its deadline. Off by
	// default for the same reason.
	RetryOnTimeout bool
}

// Retryable reports whether a failure class should produce another attempt,
// ignoring the attempt budget (which the caller enforces separately).
func (p RetryPolicy) Retryable(c FailureClass) bool {
	switch c {
	case FailureWorkerLost, FailureResourceError, FailureSystemError:
		return true
	case FailureProcessExit:
		return p.RetryOnProcessExit
	case FailureTimeout:
		return p.RetryOnTimeout
	case FailureCanceled, FailureNone:
		return false
	}
	return false
}

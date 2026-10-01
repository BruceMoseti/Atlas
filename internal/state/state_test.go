package state

import "testing"

// TestTerminalStatesAreAbsorbing is invariant I1 expressed as a test over the whole
// transition table rather than over the cases we happened to think of.
func TestTerminalStatesAreAbsorbing(t *testing.T) {
	for _, from := range AllJobStates() {
		if !from.IsTerminal() {
			continue
		}
		for _, to := range AllJobStates() {
			if _, err := Transition(from, to); err == nil {
				t.Errorf("job: %s -> %s was allowed, but %s is terminal", from, to, from)
			}
		}
	}
	for _, from := range AllAttemptStates() {
		if !from.IsTerminal() {
			continue
		}
		for _, to := range AllAttemptStates() {
			if _, err := TransitionAttempt(from, to); err == nil {
				t.Errorf("attempt: %s -> %s was allowed, but %s is terminal", from, to, from)
			}
		}
	}
}

func TestEveryNonTerminalJobStateCanReachATerminalState(t *testing.T) {
	// A state with no path to a terminal state would strand jobs forever, which
	// is a quieter and nastier bug than an illegal transition.
	reachesTerminal := map[JobState]bool{}
	for _, s := range AllJobStates() {
		if s.IsTerminal() {
			reachesTerminal[s] = true
		}
	}
	for i := 0; i < len(AllJobStates()); i++ {
		for from, tos := range jobTransitions {
			for to := range tos {
				if reachesTerminal[to] {
					reachesTerminal[from] = true
				}
			}
		}
	}
	for _, s := range AllJobStates() {
		if !reachesTerminal[s] {
			t.Errorf("job state %s cannot reach any terminal state", s)
		}
	}
}

func TestJobTransitions(t *testing.T) {
	legal := []struct{ from, to JobState }{
		{JobSubmitted, JobQueued},
		{JobQueued, JobAssigned},
		{JobAssigned, JobRunning},
		{JobRunning, JobSucceeded},
		{JobRunning, JobFailed},
		{JobRunning, JobCanceled},
		// Lease expiry sends a running job back to the queue for another
		// attempt. This edge is the whole point of the design.
		{JobRunning, JobQueued},
		{JobAssigned, JobQueued},
		// Retry exhaustion is recorded as QUEUED -> FAILED.
		{JobQueued, JobFailed},
	}
	for _, c := range legal {
		if got, err := Transition(c.from, c.to); err != nil || got != c.to {
			t.Errorf("Transition(%s, %s) = (%s, %v), want (%s, nil)", c.from, c.to, got, err, c.to)
		}
	}

	illegal := []struct{ from, to JobState }{
		{JobSucceeded, JobRunning},
		{JobFailed, JobQueued},
		{JobCanceled, JobRunning},
		{JobQueued, JobRunning},   // must be assigned first
		{JobQueued, JobSucceeded}, // cannot succeed without running
		{JobSubmitted, JobRunning},
	}
	for _, c := range illegal {
		got, err := Transition(c.from, c.to)
		if err == nil {
			t.Errorf("Transition(%s, %s) was allowed but should not be", c.from, c.to)
		}
		if got != c.from {
			t.Errorf("Transition(%s, %s) returned state %s; a rejected transition must not change state", c.from, c.to, got)
		}
	}
}

func TestUnknownStatesAreRejected(t *testing.T) {
	if _, err := Transition(JobState("WAT"), JobQueued); err == nil {
		t.Error("transition from an unknown state was allowed")
	}
	if _, err := Transition(JobQueued, JobState("WAT")); err == nil {
		t.Error("transition to an unknown state was allowed")
	}
}

func TestRetryPolicy(t *testing.T) {
	strict := RetryPolicy{}
	// Infrastructure failures are retried: the workload never got a fair run.
	for _, c := range []FailureClass{FailureWorkerLost, FailureResourceError, FailureSystemError} {
		if !strict.Retryable(c) {
			t.Errorf("%s should be retryable by default", c)
		}
	}
	// A deterministic program that exits 1 will exit 1 again.
	for _, c := range []FailureClass{FailureProcessExit, FailureTimeout, FailureCanceled, FailureNone} {
		if strict.Retryable(c) {
			t.Errorf("%s should not be retryable by default", c)
		}
	}

	lenient := RetryPolicy{RetryOnProcessExit: true, RetryOnTimeout: true}
	if !lenient.Retryable(FailureProcessExit) || !lenient.Retryable(FailureTimeout) {
		t.Error("opt-in retry classes were not honoured")
	}
	if lenient.Retryable(FailureCanceled) {
		t.Error("cancellation must never be retried, whatever the policy says")
	}
}

func TestWorkerSchedulability(t *testing.T) {
	// Only HEALTHY takes new work. SUSPECT is excluded precisely because we are
	// unsure about it, and draining is operator intent.
	if !WorkerHealthy.Schedulable() {
		t.Error("HEALTHY workers must be schedulable")
	}
	for _, s := range []WorkerState{WorkerSuspect, WorkerDead, WorkerDraining, WorkerDrained} {
		if s.Schedulable() {
			t.Errorf("%s workers must not receive new work", s)
		}
	}
}

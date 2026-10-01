package scheduler

import (
	"fmt"

	"github.com/BruceMoseti/Atlas/internal/state"
	"github.com/BruceMoseti/Atlas/internal/types"
)

// AdmissionConfig bounds what the control plane will accept.
//
// Admission control is how Atlas applies backpressure. Without it an overloaded
// cluster degrades into an unbounded queue: submissions keep succeeding, wait times
// grow without limit, and the client has no signal that anything is wrong. Refusing
// work with RESOURCE_EXHAUSTED is a worse answer for one caller and a far better one
// for the system, because the caller can shed, retry later, or route elsewhere.
type AdmissionConfig struct {
	// MaxQueueDepth is the largest number of QUEUED jobs Atlas will hold. Zero
	// disables the limit.
	MaxQueueDepth int
	// MaxInFlightPerClient bounds non-terminal jobs per client_id, so one noisy
	// client cannot consume the whole queue. Zero disables the limit.
	MaxInFlightPerClient int
	// MaxJobCPUMillis and MaxJobMemoryBytes cap a single job's request. Zero
	// disables the cap.
	MaxJobCPUMillis   int64
	MaxJobMemoryBytes int64
	// RejectUnschedulable refuses a job that no registered worker could ever run
	// even when completely idle. Such a job would otherwise sit in the queue
	// forever, consuming a slot and misleading the operator about demand.
	RejectUnschedulable bool
	// DefaultMaxAttempts is applied when a submission does not specify one.
	DefaultMaxAttempts int32
}

// DefaultAdmissionConfig returns limits suitable for a single-node cluster. They are
// deliberately finite: an "unlimited" default is how queues become unbounded.
func DefaultAdmissionConfig() AdmissionConfig {
	return AdmissionConfig{
		MaxQueueDepth:        100_000,
		MaxInFlightPerClient: 10_000,
		MaxJobCPUMillis:      0,
		MaxJobMemoryBytes:    0,
		RejectUnschedulable:  true,
		DefaultMaxAttempts:   3,
	}
}

func (c *AdmissionConfig) applyDefaults() {
	if c.DefaultMaxAttempts <= 0 {
		c.DefaultMaxAttempts = 3
	}
}

// checkRequestSize validates a job's resource request against the per-job caps.
func (c AdmissionConfig) checkRequestSize(r types.Resources) *AdmissionError {
	if r.CPUMillis <= 0 || r.MemoryBytes <= 0 {
		return &AdmissionError{
			Reason:  "invalid_request",
			Message: fmt.Sprintf("cpu_millis and memory_bytes must both be positive (got cpu=%d mem=%d)", r.CPUMillis, r.MemoryBytes),
		}
	}
	if c.MaxJobCPUMillis > 0 && r.CPUMillis > c.MaxJobCPUMillis {
		return &AdmissionError{
			Reason:  "job_too_large",
			Message: fmt.Sprintf("cpu request %d exceeds per-job limit %d", r.CPUMillis, c.MaxJobCPUMillis),
		}
	}
	if c.MaxJobMemoryBytes > 0 && r.MemoryBytes > c.MaxJobMemoryBytes {
		return &AdmissionError{
			Reason:  "job_too_large",
			Message: fmt.Sprintf("memory request %d exceeds per-job limit %d", r.MemoryBytes, c.MaxJobMemoryBytes),
		}
	}
	return nil
}

// couldEverRunLocked reports whether some worker's total capacity, ignoring what is
// currently allocated on it, could hold this request. It also reports whether there
// were any candidate workers at all, because an empty cluster must not cause
// rejections: workers that have not registered yet will.
//
// SUSPECT workers count. A transient heartbeat gap is a bad reason to reject work
// that will be perfectly placeable a second later.
//
// Caller holds s.mu.
func (s *Scheduler) couldEverRunLocked(r types.Resources) (fits bool, haveCandidates bool) {
	for _, ws := range s.workers {
		if !ws.state.Schedulable() && ws.state != state.WorkerSuspect {
			continue
		}
		haveCandidates = true
		if r.Fits(ws.view.Capacity) {
			return true, true
		}
	}
	return false, haveCandidates
}

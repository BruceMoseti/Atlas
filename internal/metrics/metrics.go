// Package metrics defines Atlas's Prometheus instrumentation.
//
// The metric set is chosen so that the four questions an operator actually asks can
// be answered without reading logs: is work flowing, is the fleet healthy, is the
// cluster full, and how long does recovery take.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds every collector Atlas exports. Call sites never nil-check it; tests
// and the simulator use NewNop to get a throwaway registry instead.
type Metrics struct {
	JobsSubmitted *prometheus.CounterVec
	JobsCompleted *prometheus.CounterVec
	JobsRejected  *prometheus.CounterVec
	JobsRunning   prometheus.Gauge
	JobsQueued    *prometheus.GaugeVec

	AttemptsTotal     *prometheus.CounterVec
	RetriesTotal      *prometheus.CounterVec
	LeaseExpirations  prometheus.Counter
	StaleRejections   *prometheus.CounterVec
	DuplicateExecs    prometheus.Counter
	IdempotentReplays *prometheus.CounterVec

	WorkersTotal   prometheus.Gauge
	WorkersByState *prometheus.GaugeVec

	QueueDepth        prometheus.Gauge
	ScheduleLatency   prometheus.Histogram
	DispatchDecisions *prometheus.CounterVec

	JobWaitSeconds    prometheus.Histogram
	JobRuntimeSeconds prometheus.Histogram

	WorkerCPUAllocated prometheus.Gauge
	WorkerMemAllocated prometheus.Gauge
	WorkerCPUCapacity  prometheus.Gauge
	WorkerMemCapacity  prometheus.Gauge
	CPUUtilization     prometheus.Gauge
	MemUtilization     prometheus.Gauge

	RecoveryLatency prometheus.Histogram
}

// New builds the collectors and registers them with reg. Pass nil to use the default
// registry.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	f := promauto(reg)

	m := &Metrics{
		JobsSubmitted: f.counterVec("atlas_jobs_submitted_total",
			"Jobs accepted by the API, by whether the idempotency key deduplicated them.", "deduplicated"),
		JobsCompleted: f.counterVec("atlas_jobs_completed_total",
			"Jobs that reached a terminal state, by terminal state and failure class.", "state", "failure_class"),
		JobsRejected: f.counterVec("atlas_jobs_rejected_total",
			"Submissions refused by admission control, by reason.", "reason"),
		JobsRunning: f.gauge("atlas_jobs_running", "Jobs currently assigned or running."),
		JobsQueued: f.gaugeVec("atlas_jobs_queued",
			"Queued jobs, split by whether retry backoff makes them dispatchable right now.", "eligible"),

		AttemptsTotal: f.counterVec("atlas_attempts_total",
			"Physical execution attempts created, by terminal outcome ('created' when started).", "outcome"),
		RetriesTotal: f.counterVec("atlas_retries_total",
			"Requeues caused by a retryable failure, by failure class.", "failure_class"),
		LeaseExpirations: f.counter("atlas_lease_expirations_total",
			"Leases reclaimed because they were not renewed in time."),
		StaleRejections: f.counterVec("atlas_stale_attempt_rejections_total",
			"Worker RPCs rejected because they referenced a non-current attempt.", "rpc"),
		DuplicateExecs: f.counter("atlas_duplicate_executions_detected_total",
			"Late successful reports for reclaimed attempts: observed duplicate physical executions."),
		IdempotentReplays: f.counterVec("atlas_idempotent_replays_total",
			"Worker RPCs that replayed an already-recorded outcome.", "rpc"),

		WorkersTotal:   f.gauge("atlas_workers_total", "Registered workers."),
		WorkersByState: f.gaugeVec("atlas_workers_by_state", "Registered workers by scheduler-believed state.", "state"),

		QueueDepth: f.gauge("atlas_queue_depth", "Jobs in the ready queue, including those in backoff."),
		ScheduleLatency: f.histogram("atlas_schedule_latency_seconds",
			"Time from a job becoming dispatchable to being assigned to a worker.",
			prometheus.ExponentialBuckets(0.0005, 2.5, 14)),
		DispatchDecisions: f.counterVec("atlas_dispatch_decisions_total",
			"Placement decisions, by outcome.", "outcome"),

		JobWaitSeconds: f.histogram("atlas_job_wait_seconds",
			"Time a job spent queued before its first assignment.",
			prometheus.ExponentialBuckets(0.001, 2.5, 16)),
		JobRuntimeSeconds: f.histogram("atlas_job_runtime_seconds",
			"Wall time of a successful attempt.",
			prometheus.ExponentialBuckets(0.001, 2.5, 16)),

		WorkerCPUAllocated: f.gauge("atlas_worker_cpu_allocated_millis", "Fleet-wide allocated CPU in millicores."),
		WorkerMemAllocated: f.gauge("atlas_worker_memory_allocated_bytes", "Fleet-wide allocated memory in bytes."),
		WorkerCPUCapacity:  f.gauge("atlas_worker_cpu_capacity_millis", "Fleet-wide schedulable CPU in millicores."),
		WorkerMemCapacity:  f.gauge("atlas_worker_memory_capacity_bytes", "Fleet-wide schedulable memory in bytes."),
		CPUUtilization:     f.gauge("atlas_cpu_utilization_ratio", "Allocated CPU divided by schedulable CPU."),
		MemUtilization:     f.gauge("atlas_memory_utilization_ratio", "Allocated memory divided by schedulable memory."),

		RecoveryLatency: f.histogram("atlas_recovery_latency_seconds",
			"Time from an attempt's lease lapsing to the job being requeued or failed.",
			prometheus.ExponentialBuckets(0.01, 2, 14)),
	}
	return m
}

// NewNop returns a fully functional metric set attached to a private registry, for
// callers that want the instrumentation to work but do not want to export it.
func NewNop() *Metrics { return New(prometheus.NewRegistry()) }

type factory struct{ reg prometheus.Registerer }

func promauto(reg prometheus.Registerer) factory { return factory{reg} }

func (f factory) counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	f.reg.MustRegister(c)
	return c
}

func (f factory) counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(c)
	return c
}

func (f factory) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	f.reg.MustRegister(g)
	return g
}

func (f factory) gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(g)
	return g
}

func (f factory) histogram(name, help string, buckets []float64) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
	f.reg.MustRegister(h)
	return h
}

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/BruceMoseti/Atlas/internal/scheduler"
	"github.com/BruceMoseti/Atlas/internal/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HealthHandler serves the operational HTTP surface: liveness, readiness, metrics,
// and a human-readable status page.
//
// Liveness and readiness are different questions and conflating them is a classic
// way to turn a recoverable dependency outage into a crash loop. Atlas answers:
//
//	/live   is the process running?        -> yes unless the process is gone
//	/ready  can it safely accept work?     -> only if its database answers
//
// A scheduler whose disk has gone away is alive but must not be sent traffic.
type HealthHandler struct {
	store *store.Store
	sched *scheduler.Scheduler
	reg   prometheus.Gatherer
}

// NewHealthHandler builds the HTTP mux for /live, /ready, /metrics, and /status.
func NewHealthHandler(st *store.Store, sched *scheduler.Scheduler, reg prometheus.Gatherer) *http.ServeMux {
	h := &HealthHandler{store: st, sched: sched, reg: reg}
	mux := http.NewServeMux()
	mux.HandleFunc("/live", h.live)
	mux.HandleFunc("/ready", h.ready)
	mux.HandleFunc("/status", h.status)
	if reg != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	}
	return mux
}

func (h *HealthHandler) live(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (h *HealthHandler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	err := h.store.View(ctx, func(tx *store.Tx) error {
		_, err := tx.CountJobsInStates("", "QUEUED")
		return err
	})
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("store unavailable: " + err.Error() + "\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func (h *HealthHandler) status(w http.ResponseWriter, _ *http.Request) {
	st := h.sched.Status()
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(map[string]any{
		"workers_total":      st.WorkersTotal,
		"workers_healthy":    st.WorkersHealthy,
		"workers_by_state":   st.WorkersByState,
		"cpu_capacity_milli": st.Capacity.CPUMillis,
		"cpu_allocated_mill": st.Allocated.CPUMillis,
		"memory_capacity":    st.Capacity.MemoryBytes,
		"memory_allocated":   st.Allocated.MemoryBytes,
		"cpu_utilization":    st.CPUUtilization,
		"memory_utilization": st.MemUtilization,
		"jobs_queued":        st.JobsQueued,
		"jobs_queued_ready":  st.JobsQueuedReady,
		"jobs_running":       st.JobsRunning,
		"scheduling_policy":  st.SchedulingPolicy,
		"queue_ordering":     st.QueueOrdering,
	})
}

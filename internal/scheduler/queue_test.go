package scheduler

import (
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/types"
)

var base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func qj(id string, prio int32, enqueuedOffset time.Duration) *QueuedJob {
	t := base.Add(enqueuedOffset)
	return &QueuedJob{
		JobID:      id,
		Priority:   prio,
		Request:    types.Resources{CPUMillis: 1000, MemoryBytes: 1 << 20},
		EnqueuedAt: t,
		EligibleAt: t,
		heapIndex:  -1,
	}
}

func drain(t *testing.T, q *ReadyQueue, now time.Time) []string {
	t.Helper()
	var out []string
	for {
		j := q.PopBest(now)
		if j == nil {
			return out
		}
		out = append(out, j.JobID)
	}
}

func TestFIFOIgnoresPriority(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderFIFO})
	q.Push(qj("a", 0, 0))
	q.Push(qj("b", 100, time.Second))
	q.Push(qj("c", 50, 2*time.Second))

	got := drain(t, q, base.Add(time.Minute))
	want := []string{"a", "b", "c"}
	assertOrder(t, got, want)
}

func TestStrictPriorityOrdersByPriorityThenAge(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderPriority})
	q.Push(qj("low", 1, 0))
	q.Push(qj("high-late", 10, 2*time.Second))
	q.Push(qj("high-early", 10, time.Second))

	got := drain(t, q, base.Add(time.Minute))
	assertOrder(t, got, []string{"high-early", "high-late", "low"})
}

// TestPriorityAgingPreventsStarvation is the behavioural difference between
// OrderPriority and OrderPriorityAging: an old low-priority job must eventually
// outrank a freshly arrived high-priority one.
func TestPriorityAgingPreventsStarvation(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderPriorityAging, AgingRate: 1, AgingCap: 100})
	old := qj("old-low", 1, 0)
	fresh := qj("fresh-high", 50, 100*time.Second)
	q.Push(old)
	q.Push(fresh)

	// At t=100s the low-priority job has aged 100 points on top of its base 1,
	// so 101 beats the newcomer's 50.
	if got := q.PopBest(base.Add(100 * time.Second)); got.JobID != "old-low" {
		t.Fatalf("after 100s of aging the starved job should run first, got %s", got.JobID)
	}

	// Under strict priority the same pair resolves the other way, which is what
	// makes the comparison in docs/RESULTS.md meaningful.
	q2 := NewReadyQueue(QueueConfig{Ordering: OrderPriority})
	q2.Push(qj("old-low", 1, 0))
	q2.Push(qj("fresh-high", 50, 100*time.Second))
	if got := q2.PopBest(base.Add(100 * time.Second)); got.JobID != "fresh-high" {
		t.Fatalf("strict priority should still favour the high-priority job, got %s", got.JobID)
	}
}

func TestAgingCapBoundsTheBonus(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderPriorityAging, AgingRate: 1, AgingCap: 5})
	j := qj("j", 0, 0)
	q.Push(j)
	// 1000 seconds of waiting, but the cap stops the bonus at 5.
	if got := q.EffectivePriority(j, base.Add(1000*time.Second)); got != 5 {
		t.Fatalf("effective priority = %v, want 5 (base 0 + capped bonus)", got)
	}
}

func TestEDFOrdersByDeadlineAndSortsUndeadlinedLast(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderEDF})
	far := qj("far", 0, 0)
	far.Deadline = base.Add(time.Hour)
	near := qj("near", 0, 0)
	near.Deadline = base.Add(time.Minute)
	none := qj("none", 0, 0)

	q.Push(far)
	q.Push(none)
	q.Push(near)

	assertOrder(t, drain(t, q, base), []string{"near", "far", "none"})
}

func TestBackoffJobsAreHeldUntilEligible(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderPriorityAging, AgingRate: 1})
	delayed := qj("delayed", 100, 0)
	delayed.EligibleAt = base.Add(10 * time.Second)
	q.Push(delayed)
	q.Push(qj("ready", 0, 0))

	if q.Ready() != 1 || q.Delayed() != 1 {
		t.Fatalf("Ready=%d Delayed=%d, want 1 and 1", q.Ready(), q.Delayed())
	}
	// The high-priority job is in backoff, so the low-priority one runs first.
	if got := q.PopBest(base); got.JobID != "ready" {
		t.Fatalf("got %s, want the only eligible job", got.JobID)
	}
	if got := q.PopBest(base); got != nil {
		t.Fatalf("got %s, want nil: the remaining job is still in backoff", got.JobID)
	}

	if n := q.PromoteEligible(base.Add(10 * time.Second)); n != 1 {
		t.Fatalf("PromoteEligible returned %d, want 1", n)
	}
	if got := q.PopBest(base.Add(10 * time.Second)); got == nil || got.JobID != "delayed" {
		t.Fatalf("the delayed job should be dispatchable once its backoff elapses")
	}
}

func TestNextDelayedAtReportsTheEarliestWakeup(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderFIFO})
	if !q.NextDelayedAt().IsZero() {
		t.Fatal("an empty queue has no pending wakeup")
	}
	late := qj("late", 0, 0)
	late.EligibleAt = base.Add(30 * time.Second)
	soon := qj("soon", 0, 0)
	soon.EligibleAt = base.Add(5 * time.Second)
	q.Push(late)
	q.Push(soon)

	if got := q.NextDelayedAt(); !got.Equal(base.Add(5 * time.Second)) {
		t.Fatalf("NextDelayedAt = %v, want the soonest eligibility", got)
	}
}

// TestRequeuePreservesOrder covers the dispatcher's backfill path: jobs it could not
// place must go back exactly where they were, or a large job would drift later every
// sweep and never run.
func TestRequeuePreservesOrder(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderFIFO})
	for _, id := range []string{"a", "b", "c", "d"} {
		q.Push(qj(id, 0, time.Duration(len(id))*time.Second))
	}

	popped := []*QueuedJob{q.PopBest(base), q.PopBest(base)}
	q.Requeue(popped)

	assertOrder(t, drain(t, q, base), []string{"a", "b", "c", "d"})
}

func TestRemoveSupportsCancellation(t *testing.T) {
	for _, ordering := range []Ordering{OrderFIFO, OrderPriority, OrderPriorityAging, OrderEDF} {
		q := NewReadyQueue(QueueConfig{Ordering: ordering})
		q.Push(qj("keep", 0, 0))
		q.Push(qj("drop", 0, time.Second))

		if !q.Remove("drop") {
			t.Fatalf("%s: Remove reported the job was absent", ordering)
		}
		if q.Remove("drop") {
			t.Fatalf("%s: Remove reported success twice for the same job", ordering)
		}
		if q.Len() != 1 {
			t.Fatalf("%s: Len = %d after removing one of two", ordering, q.Len())
		}
		assertOrder(t, drain(t, q, base.Add(time.Minute)), []string{"keep"})
	}
}

func TestRemoveFromBackoffWorks(t *testing.T) {
	q := NewReadyQueue(QueueConfig{Ordering: OrderPriorityAging})
	j := qj("delayed", 0, 0)
	j.EligibleAt = base.Add(time.Minute)
	q.Push(j)

	if !q.Remove("delayed") {
		t.Fatal("a job in retry backoff should still be cancellable")
	}
	if q.Len() != 0 || q.Delayed() != 0 {
		t.Fatalf("Len=%d Delayed=%d after removal, want 0 and 0", q.Len(), q.Delayed())
	}
}

func TestPushIsIdempotent(t *testing.T) {
	// Requeue paths can race with the periodic resync, so a double push must not
	// duplicate a job.
	q := NewReadyQueue(QueueConfig{Ordering: OrderFIFO})
	q.Push(qj("a", 0, 0))
	q.Push(qj("a", 0, 0))
	if q.Len() != 1 {
		t.Fatalf("Len = %d after pushing the same job twice, want 1", q.Len())
	}
}

func assertOrder(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

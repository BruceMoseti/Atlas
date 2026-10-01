package scheduler

import (
	"container/heap"
	"container/list"
	"fmt"
	"strings"
	"time"

	"github.com/BruceMoseti/Atlas/internal/types"
)

// Ordering decides which queued job the dispatcher considers first.
type Ordering string

const (
	// OrderFIFO ignores priority entirely: oldest job first.
	OrderFIFO Ordering = "fifo"
	// OrderPriority is strict priority, oldest first within a priority. It can
	// starve low-priority work indefinitely, which is why aging exists.
	OrderPriority Ordering = "priority"
	// OrderPriorityAging adds AgingRate priority points per second of queue wait,
	// capped at AgingCap, so a low-priority job eventually outranks newly arrived
	// high-priority work.
	OrderPriorityAging Ordering = "priority-aging"
	// OrderEDF is earliest-deadline-first. Jobs without a deadline sort last.
	OrderEDF Ordering = "edf"
)

// ParseOrdering resolves a queue ordering by name.
func ParseOrdering(s string) (Ordering, error) {
	switch Ordering(strings.ToLower(strings.TrimSpace(s))) {
	case "", OrderPriorityAging:
		return OrderPriorityAging, nil
	case OrderFIFO:
		return OrderFIFO, nil
	case OrderPriority:
		return OrderPriority, nil
	case OrderEDF:
		return OrderEDF, nil
	default:
		return "", fmt.Errorf("unknown queue ordering %q (want fifo, priority, priority-aging, or edf)", s)
	}
}

// OrderingNames lists the orderings ParseOrdering accepts.
func OrderingNames() []string {
	return []string{string(OrderFIFO), string(OrderPriority), string(OrderPriorityAging), string(OrderEDF)}
}

// QueuedJob is the dispatcher's view of a waiting job. It carries only what placement
// needs, so the hot loop never touches the database.
type QueuedJob struct {
	JobID      string
	Priority   int32
	Request    types.Resources
	EnqueuedAt time.Time
	// EligibleAt is in the future while the job is in retry backoff.
	EligibleAt time.Time
	// Deadline is the zero time when the job has none.
	Deadline time.Time

	elem      *list.Element // position in its priority bucket
	heapIndex int           // position in the EDF or delay heap
}

// ReadyQueue holds every QUEUED job, split into those that are dispatchable now and
// those still in retry backoff.
//
// It is not safe for concurrent use; the scheduler holds its own lock across every
// call. Keeping the queue lock-free avoids a second lock ordering to reason about.
//
// The structure is a map of per-priority deques sorted by enqueue time rather than
// one big heap, and that choice is load-bearing. For FIFO, strict priority, and
// priority-with-aging, the best job in a bucket is always the bucket's oldest job,
// because all three orderings are monotone in wait time within a fixed base
// priority. So the global best is found by comparing bucket heads, which is
// O(distinct priorities) and does not depend on how many jobs are queued. A heap
// cannot do this, because aging changes every key continuously.
//
// Earliest-deadline-first breaks that monotonicity, so it gets a real heap.
type ReadyQueue struct {
	ordering  Ordering
	agingRate float64
	agingCap  float64

	buckets map[int32]*list.List
	edf     *edfHeap
	delayed *delayHeap

	index map[string]*QueuedJob
}

// QueueConfig configures a ReadyQueue.
type QueueConfig struct {
	Ordering Ordering
	// AgingRate is priority points gained per second of queue wait.
	AgingRate float64
	// AgingCap bounds the gain so that aging never fully inverts the priority
	// scheme.
	AgingCap float64
}

// NewReadyQueue builds an empty queue.
func NewReadyQueue(cfg QueueConfig) *ReadyQueue {
	if cfg.Ordering == "" {
		cfg.Ordering = OrderPriorityAging
	}
	return &ReadyQueue{
		ordering:  cfg.Ordering,
		agingRate: cfg.AgingRate,
		agingCap:  cfg.AgingCap,
		buckets:   make(map[int32]*list.List),
		edf:       &edfHeap{},
		delayed:   &delayHeap{},
		index:     make(map[string]*QueuedJob),
	}
}

// Len is the total number of queued jobs, including those in backoff.
func (q *ReadyQueue) Len() int { return len(q.index) }

// Ready is the number of jobs that are dispatchable now.
func (q *ReadyQueue) Ready() int { return len(q.index) - q.delayed.Len() }

// Delayed is the number of jobs waiting out retry backoff.
func (q *ReadyQueue) Delayed() int { return q.delayed.Len() }

// Contains reports queue membership.
func (q *ReadyQueue) Contains(jobID string) bool { _, ok := q.index[jobID]; return ok }

// Push adds a job. A job already in the queue is ignored, which makes requeue paths
// idempotent.
func (q *ReadyQueue) Push(j *QueuedJob) {
	if _, dup := q.index[j.JobID]; dup {
		return
	}
	q.index[j.JobID] = j
	if j.EligibleAt.After(j.EnqueuedAt) {
		heap.Push(q.delayed, j)
		return
	}
	q.pushReady(j)
}

// pushReady inserts a job into its bucket, keeping the bucket sorted by enqueue
// time. Sorted insertion is what makes "the bucket head is the bucket's best job"
// true for every ordering that uses buckets, and the common case — a job enqueued
// now, which is newer than everything already queued — is a single append.
//
// Out-of-order pushes are not hypothetical: recovery and the periodic resync load
// rows from the database in whatever order the query returns them.
func (q *ReadyQueue) pushReady(j *QueuedJob) {
	if q.ordering == OrderEDF {
		heap.Push(q.edf, j)
		return
	}
	j.heapIndex = -1
	b, ok := q.buckets[j.Priority]
	if !ok {
		b = list.New()
		q.buckets[j.Priority] = b
	}
	for e := b.Back(); e != nil; e = e.Prev() {
		if !j.EnqueuedAt.Before(e.Value.(*QueuedJob).EnqueuedAt) {
			j.elem = b.InsertAfter(j, e)
			return
		}
	}
	j.elem = b.PushFront(j)
}

// pushReadyFront returns a job to the head of its bucket, preserving the order it was
// popped in. The dispatcher uses this for jobs it could not place.
func (q *ReadyQueue) pushReadyFront(j *QueuedJob) {
	if q.ordering == OrderEDF {
		heap.Push(q.edf, j)
		return
	}
	b, ok := q.buckets[j.Priority]
	if !ok {
		b = list.New()
		q.buckets[j.Priority] = b
	}
	j.elem = b.PushFront(j)
}

// Remove drops a job from the queue, for cancellation. It reports whether the job was
// present.
func (q *ReadyQueue) Remove(jobID string) bool {
	j, ok := q.index[jobID]
	if !ok {
		return false
	}
	delete(q.index, jobID)
	switch {
	case j.elem != nil:
		q.buckets[j.Priority].Remove(j.elem)
		j.elem = nil
	case q.ordering == OrderEDF && j.heapIndex >= 0 && j.heapIndex < q.edf.Len() && (*q.edf)[j.heapIndex] == j:
		heap.Remove(q.edf, j.heapIndex)
	case j.heapIndex >= 0 && j.heapIndex < q.delayed.Len() && (*q.delayed)[j.heapIndex] == j:
		heap.Remove(q.delayed, j.heapIndex)
	}
	return true
}

// PromoteEligible moves jobs whose backoff has elapsed into the dispatchable set and
// returns how many moved.
func (q *ReadyQueue) PromoteEligible(now time.Time) int {
	n := 0
	for q.delayed.Len() > 0 {
		head := (*q.delayed)[0]
		if head.EligibleAt.After(now) {
			break
		}
		heap.Pop(q.delayed)
		q.pushReady(head)
		n++
	}
	return n
}

// NextDelayedAt returns when the earliest backed-off job becomes eligible, or the
// zero time if none are waiting. The dispatcher uses it to sleep exactly long enough.
func (q *ReadyQueue) NextDelayedAt() time.Time {
	if q.delayed.Len() == 0 {
		return time.Time{}
	}
	return (*q.delayed)[0].EligibleAt
}

// PopBest removes and returns the highest-ranked dispatchable job, or nil.
func (q *ReadyQueue) PopBest(now time.Time) *QueuedJob {
	if q.ordering == OrderEDF {
		if q.edf.Len() == 0 {
			return nil
		}
		j := heap.Pop(q.edf).(*QueuedJob)
		delete(q.index, j.JobID)
		return j
	}

	var best *QueuedJob
	var bestScore float64
	for prio, b := range q.buckets {
		if b.Len() == 0 {
			delete(q.buckets, prio)
			continue
		}
		head := b.Front().Value.(*QueuedJob)
		score := q.score(head, now)
		if best == nil || score > bestScore ||
			// Deterministic tie-break so that benchmark runs are reproducible.
			(score == bestScore && head.EnqueuedAt.Before(best.EnqueuedAt)) {
			best, bestScore = head, score
		}
	}
	if best == nil {
		return nil
	}
	q.buckets[best.Priority].Remove(best.elem)
	best.elem = nil
	delete(q.index, best.JobID)
	return best
}

// Requeue returns previously popped jobs to the front of the queue in their original
// relative order. Call it with the jobs in the order they were popped.
func (q *ReadyQueue) Requeue(jobs []*QueuedJob) {
	for i := len(jobs) - 1; i >= 0; i-- {
		j := jobs[i]
		if _, dup := q.index[j.JobID]; dup {
			continue
		}
		q.index[j.JobID] = j
		q.pushReadyFront(j)
	}
}

// score ranks a job; higher wins. Only meaningful for the bucketed orderings.
func (q *ReadyQueue) score(j *QueuedJob, now time.Time) float64 {
	switch q.ordering {
	case OrderFIFO:
		// Oldest first, independent of priority.
		return -float64(j.EnqueuedAt.UnixNano())
	case OrderPriority:
		return float64(j.Priority)
	default: // OrderPriorityAging
		wait := now.Sub(j.EnqueuedAt).Seconds()
		if wait < 0 {
			wait = 0
		}
		bonus := q.agingRate * wait
		if q.agingCap > 0 && bonus > q.agingCap {
			bonus = q.agingCap
		}
		return float64(j.Priority) + bonus
	}
}

// EffectivePriority exposes the aging calculation for tests and for the CLI's queue
// view.
func (q *ReadyQueue) EffectivePriority(j *QueuedJob, now time.Time) float64 {
	return q.score(j, now)
}

// ---------------------------------------------------------------- heaps

// edfHeap orders by deadline, then by enqueue time. Jobs with no deadline sort last.
type edfHeap []*QueuedJob

func (h edfHeap) Len() int { return len(h) }

func (h edfHeap) Less(i, j int) bool {
	di, dj := h[i].Deadline, h[j].Deadline
	switch {
	case di.IsZero() && dj.IsZero():
		return h[i].EnqueuedAt.Before(h[j].EnqueuedAt)
	case di.IsZero():
		return false
	case dj.IsZero():
		return true
	case di.Equal(dj):
		return h[i].EnqueuedAt.Before(h[j].EnqueuedAt)
	default:
		return di.Before(dj)
	}
}

func (h edfHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex, h[j].heapIndex = i, j
}

func (h *edfHeap) Push(x any) {
	j := x.(*QueuedJob)
	j.heapIndex = len(*h)
	*h = append(*h, j)
}

func (h *edfHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	old[n-1] = nil
	j.heapIndex = -1
	*h = old[:n-1]
	return j
}

// delayHeap orders by eligibility time: the next job to come out of backoff is first.
type delayHeap []*QueuedJob

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].EligibleAt.Before(h[j].EligibleAt) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].heapIndex, h[j].heapIndex = i, j }
func (h *delayHeap) Push(x any)        { j := x.(*QueuedJob); j.heapIndex = len(*h); *h = append(*h, j) }
func (h *delayHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	old[n-1] = nil
	j.heapIndex = -1
	*h = old[:n-1]
	return j
}

package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/BruceMoseti/Atlas/internal/types"
)

// These microbenchmarks cover the two things the dispatcher does on every job: pick
// the next candidate out of the queue, and pick a worker for it. Everything else in
// the scheduling path is a database write, which is measured end to end by the chaos
// harness instead.

func benchFleet(n int, occupancy float64) []*WorkerView {
	const capCPU, capMem = int64(16000), int64(32) << 30
	fleet := make([]*WorkerView, n)
	for i := range fleet {
		capacity := types.Resources{CPUMillis: capCPU, MemoryBytes: capMem}
		used := occupancy * float64(i%7) / 6
		fleet[i] = &WorkerView{
			ID:       fmt.Sprintf("w-%d", i),
			Capacity: capacity,
			Available: types.Resources{
				CPUMillis:   capCPU - int64(float64(capCPU)*used),
				MemoryBytes: capMem - int64(float64(capMem)*used),
			},
			Schedulable: true,
		}
	}
	return fleet
}

func BenchmarkPolicySelect(b *testing.B) {
	req := types.Resources{CPUMillis: 500, MemoryBytes: 512 << 20}
	for _, n := range []int{10, 100, 1000, 10000} {
		fleet := benchFleet(n, 0.8)
		for _, name := range PolicyNames() {
			policy, err := NewPolicy(name)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("%s/workers=%d", name, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					policy.Select(req, fleet)
				}
			})
		}
	}
}

// BenchmarkQueuePopBest measures the claim the queue's design rests on: finding the
// best job costs O(distinct priorities), not O(queued jobs).
func BenchmarkQueuePopBest(b *testing.B) {
	for _, depth := range []int{100, 10000, 100000} {
		for _, ordering := range []Ordering{OrderFIFO, OrderPriorityAging, OrderEDF} {
			b.Run(fmt.Sprintf("%s/depth=%d", ordering, depth), func(b *testing.B) {
				now := time.Now()
				q := NewReadyQueue(QueueConfig{Ordering: ordering, AgingRate: 1, AgingCap: 100})
				jobs := make([]*QueuedJob, depth)
				for i := range jobs {
					t := now.Add(time.Duration(i) * time.Millisecond)
					jobs[i] = &QueuedJob{
						JobID:      fmt.Sprintf("job-%d", i),
						Priority:   int32(i % 8),
						Request:    types.Resources{CPUMillis: 500, MemoryBytes: 512 << 20},
						EnqueuedAt: t,
						EligibleAt: t,
						Deadline:   t.Add(time.Minute),
					}
					q.Push(jobs[i])
				}

				b.ResetTimer()
				b.ReportAllocs()
				one := make([]*QueuedJob, 1)
				for i := 0; i < b.N; i++ {
					j := q.PopBest(now)
					if j == nil {
						b.Fatal("queue drained unexpectedly")
					}
					// Requeue rather than Push, matching what the dispatcher
					// does with a job it could not place, and keeping the
					// depth constant so each iteration measures one pop at
					// that depth.
					one[0] = j
					q.Requeue(one)
				}
			})
		}
	}
}

func BenchmarkQueuePush(b *testing.B) {
	now := time.Now()
	b.ReportAllocs()
	q := NewReadyQueue(QueueConfig{Ordering: OrderPriorityAging, AgingRate: 1})
	for i := 0; i < b.N; i++ {
		t := now.Add(time.Duration(i) * time.Microsecond)
		q.Push(&QueuedJob{
			JobID:      fmt.Sprintf("job-%d", i),
			Priority:   int32(i % 8),
			EnqueuedAt: t,
			EligibleAt: t,
		})
	}
}

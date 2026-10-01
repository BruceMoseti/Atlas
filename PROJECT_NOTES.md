# Atlas — Design Notes and Interview Preparation

Companion to the [README](README.md). This document covers the reasoning behind the
system, the parts that were genuinely hard, the bugs that showed up and how they
were caught, and a question bank for discussing it.

Every claim here is traceable to committed code, a test, a benchmark result in
`results/`, or a commit in the history. Where something is unverified, it says so.

---

## 1. Problem statement

**Build a scheduler that places resource-constrained jobs on a worker fleet and
keeps them running when the fleet, the network, and the scheduler itself fail.**

The constraint that shapes everything: when a worker stops responding mid-job, the
scheduler cannot determine whether the work completed. It knows only that it did
not receive a completion. Those two worlds are observationally identical from the
control plane, and no amount of engineering collapses them — the only way to do so
would be to commit the result in the same transaction as the side effect, which is
impossible when the side effect is an arbitrary user process.

So the design question is not "how do I avoid duplicate execution" but:

> Given that duplicate execution is unavoidable, what *can* be guaranteed, how do I
> make those guarantees precise, and how do I prove the implementation honours them?

Three sub-problems follow:

1. **Ownership.** Exactly one worker must have authority over a given execution at
   any instant, and that authority must expire without needing to reach the worker.
2. **Accounting.** Resource reservations must survive crashes and must never
   oversubscribe a machine, including when a reservation's owner disappears.
3. **Evidence.** The properties must be checkable mechanically against the system's
   own durable record, not argued for in a README.

Secondary goals: resource-aware placement that is measurably better than a naive
baseline, bounded queueing under overload, and an operational surface (metrics,
health, draining) that someone could actually run.

### Non-goals, chosen deliberately

High availability, consensus, DAGs/workflows, autoscaling, multi-tenancy with real
isolation, GPU support. Each would add surface area without making the three
problems above any better solved. `docs/ARCHITECTURE.md` has the long form.

---

## 2. Architecture in one page

```
CLI ──gRPC──▶ CONTROL PLANE (one atlas-server)
                ├─ gRPC API        admission, idempotency, error→status mapping
                ├─ Dispatch loop   policy → backfill window → batched durable commit
                ├─ Reconcile loop  lease expiry · worker health · queue resync (250ms)
                └─ SQLite          WAL, synchronous=FULL, single writer
                                   jobs · attempts · workers · transitions (audit log)
                       ▲
                       │  workers always dial in — the scheduler never dials out
         ┌─────────────┴─────────────┐
   atlas-worker                atlas-worker
   register → heartbeat → acquire → renew → run → report
         │                           │
   process executor            docker executor
   (own process group)         (kernel-enforced limits)
```

**Three structural decisions:**

| | |
| --- | --- |
| **Workers pull, scheduler decides** | Every connection is worker-initiated, so no inbound reachability and no service discovery. But workers poll for assignments the dispatcher *already committed*, so placement stays centralized and the policy comparison stays a controlled experiment. |
| **The store exposes transactions, not operations** | `Update(func(*Tx) error)` lets the scheduler compose "create the attempt, move the job, reserve capacity" into one atomic fact. Two correctness rules live in the data layer: state only changes through validated transitions, and `SaveWorker` refuses oversubscription. |
| **In-memory state is a cache that is always re-validated** | The policy chooses against a snapshot; the transaction decides against rows. A stale cache costs a wasted decision and nothing else. |

---

## 3. What is in the implementation

| Component | Where | What it does |
| --- | --- | --- |
| State machines | `internal/state` | Job, attempt, and worker states and the only legal edges. No I/O. Terminal states absorbing by construction. |
| Durable store | `internal/store` | SQLite schema, transaction scope, validated transitions, append-only audit log, capacity enforcement. |
| Placement policies | `internal/scheduler/policies.go` | Round-robin, least-loaded, best-fit behind one interface. |
| Ready queue | `internal/scheduler/queue.go` | Per-priority deques (FIFO/priority/aging) + EDF heap + retry-backoff delay heap. |
| Dispatcher | `internal/scheduler/dispatch.go` | Batch construction, optimistic reservation, re-validated batched commit, cache reconciliation. |
| Lease machinery | `internal/scheduler/lease.go` | Lease expiry sweep, two-stage worker health ageing, retry disposition with jittered backoff, queue resync. |
| Worker protocol | `internal/scheduler/worker_api.go` | Registration, heartbeat reconciliation, long-poll acquire, and `validateRef` — the stale-write gate. |
| Admission control | `internal/scheduler/admission.go` | Queue depth, per-client in-flight, per-job size, could-ever-fit. |
| Recovery | `internal/scheduler/scheduler.go` | Rebuild fleet, recompute allocation, reclaim lapsed leases, rebuild queue preserving accrued priority. |
| Worker agent | `internal/worker` | Three concurrent loops, process + Docker executors, abandonment classification, fault-injection hooks. |
| gRPC layer | `internal/api` | Wire conversion and domain-error → status-code mapping. Deliberately thin. |
| Invariant checker | `internal/invariants` | Nine invariants verified against the durable record, including the audit log. |
| Simulator | `simulator/` | Discrete-event model reusing the real queue, policies, and resource model. |
| Chaos harness | `chaos/` | Real child processes, real signals, campaign report, non-zero exit on violation. |

13,628 lines of Go excluding generated protobuf, of which 3,788 are tests.

---

## 4. The hardest problem: distinguishing a stale write from a replayed one

Both look the same from the outside — a worker sending a result for an attempt the
scheduler already considers finished — and they require **opposite** responses.

```
t0  worker A assigned job J, attempt 1, lease L1
t1  A is partitioned; it keeps running
t2  L1 expires → attempt 1 LOST → J requeued
t3  J assigned to worker B as attempt 2, lease L2
t4  A's partition heals
t5  A reports "J completed successfully"
```

If Atlas accepts A's report, J is marked `SUCCEEDED` with A's exit code while B is
still running and may produce a different answer. That is state corruption.

But the *mechanically identical* case — A's report landed, the response was lost,
A retries — must succeed, or every lost acknowledgement turns into a spurious
failure or a duplicate state transition.

**The resolution** is to scope authority per-attempt and read the attempt's state:

```go
// internal/scheduler/worker_api.go
if a.State.IsTerminal() {
    // LOST means Atlas reclaimed the attempt and very likely gave the job to
    // someone else. Anything this caller says about it is stale by definition.
    // The other terminal states are outcomes this caller already reported, so
    // repeating them is an idempotent replay.
    if a.State == state.AttemptLost {
        return j, a, false, ErrStaleAttempt
    }
    return j, a, true, nil            // replay: return the recorded outcome
}
if j.CurrentAttemptID != a.ID {
    return j, a, false, ErrStaleAttempt
}
```

`LOST` versus `SUCCEEDED`/`FAILED`/`CANCELED` is the whole distinction. `LOST` is a
state Atlas assigns when it does *not* know what happened; the others are outcomes
a caller reported. Having introduced `LOST` as a first-class attempt state — rather
than folding it into `FAILED` — is what makes the discrimination a single
comparison instead of a heuristic.

Three further details:

- **The late report is counted, not discarded.** A `LOST` attempt whose owner later
  reports exit 0 is a *directly observed duplicate execution*. It increments
  `atlas_duplicate_executions_detected_total` and writes a durable marker on the
  attempt row, so the count survives the scheduler restarts the chaos campaign
  injects. The flagship run reports 16.
- **The reverse direction exists too.** Rather than waiting for a stale worker to
  report something that will be rejected, the heartbeat response carries a
  `cancel_attempt_ids` list, so the worker kills the execution as soon as the
  scheduler notices.
- **Lease ids are unguessable.** `TestForgedLeaseIsRejected` confirms that knowing
  the job and attempt ids is not sufficient authority.

---

## 5. The algorithm worth explaining: the ready queue under priority aging

**Requirement:** effective priority is `base + rate × wait`, capped. Pop the
highest-effective-priority job efficiently at queue depths up to 100,000.

**Why the obvious answer fails.** A binary heap keyed on effective priority is
wrong, because every key changes continuously as time passes. The heap property
breaks silently; `Fix` on every element each tick is O(n log n).

**The structure used instead** — a map of per-priority deques, each sorted by
enqueue time:

```
priority 100 ─▶ [oldest] ─ … ─ [newest]    ← this bucket's best job is always its head
priority  50 ─▶ [oldest] ─ … ─ [newest]
priority   0 ─▶ [oldest] ─ … ─ [newest]
                    ▲
         compare only the heads: O(distinct priorities), independent of depth
```

**Why head-comparison is exact.** Within a fixed base priority, all three bucketed
orderings are monotone in wait time:

- FIFO: key is `-enqueuedAt`, maximized by the oldest.
- Strict priority: key is constant in the bucket; ties break on age.
- Aging: `base + rate × wait` with `base` and `rate` fixed within a bucket, so the
  longest-waiting job has the largest key.

Therefore the global maximum is the maximum over bucket heads. EDF breaks this —
deadline order is not monotone in arrival order — so it gets a real heap, and that
shows up in the measurements as the expected O(log n).

**Measured** (`make bench`, `results/` and `docs/RESULTS.md`):

| Depth | fifo | priority-aging | edf |
| --- | --- | --- | --- |
| 100 | 159 ns | 192 ns | 176 ns |
| 10,000 | 172 ns | 202 ns | 348 ns |
| 100,000 | 166 ns | 213 ns | 442 ns |

Two supporting details that are easy to miss and both caused bugs:

- **Insertion must preserve bucket order.** Recovery loads rows in `created_at DESC`
  order; appending them blindly breaks the "head is oldest" invariant. `pushReady`
  does a sorted insert that is O(1) for the steady-state case (a job enqueued now is
  newer than everything) and callers rebuilding from the database sort first.
- **Jobs in retry backoff live in a separate delay heap**, so a job waiting out its
  backoff cannot block the head of a priority bucket.

---

## 6. The engineering decision I would defend hardest: no consensus

"Distributed scheduler" triggers a reflex to reach for Raft. I think that is the
wrong call here, and the reasoning generalizes.

**What Raft would buy:** the control plane stays available when the scheduler's
machine dies, rather than being unavailable until it restarts.

**What it would cost:** a replicated log, leader election, membership changes,
snapshot/compaction, and a consensus-aware rewrite of every write path. Each is a
rich source of subtle correctness bugs, and none of them improves the problems that
actually make this system interesting — lease ownership, failure detection,
resource accounting, idempotency, and proving the result.

**The asymmetry that decides it:** Atlas already survives scheduler loss *for the
jobs*. Running work keeps running on workers; state is durable; restart reconciles
everything. What is lost during a restart is the *control plane's availability*, not
the cluster's work. Trading a large correctness surface for a few seconds of API
availability is a bad trade for a system whose selling point is provable
correctness.

**What I would do instead if HA were required:** put only the *leadership lease* in
consensus using an embedded library, keep the SQLite store, and replicate it by log
shipping. That isolates consensus to one small, well-understood concern instead of
spreading it through the write path.

**The tradeoff, stated plainly:** Atlas is crash-*recoverable*, not
fault-*tolerant to scheduler loss*, and does not survive disk loss. That sentence is
in the README, in `docs/SEMANTICS.md` §8, and in `docs/ARCHITECTURE.md`.

---

## 7. Performance work and the bottleneck that mattered

**The bottleneck is `fsync`, not CPU.** Every assignment must be durable before a
worker is told about it. With `synchronous=FULL`, that is one `fsync` per
transaction, and a per-placement transaction caps assignment throughput at the
disk's fsync rate regardless of how fast the scheduling logic is.

**The fix is batching.** `dispatchOnce` builds up to 256 placements against the
in-memory fleet and commits them in a single transaction — N fsyncs become one.

This created a second problem and the solution to it is the interesting part. The
policy chooses against a *cached* fleet view, which may be stale by the time the
transaction runs. Rather than locking the fleet across the commit (which would
serialize the whole scheduler behind disk I/O), each placement is **re-validated
inside the transaction**:

```go
if j.State != state.JobQueued              { continue }      // canceled meanwhile
if !w.State.Schedulable()                  { reject }        // went SUSPECT
if !j.Request.Fits(w.Available())          { reject }        // capacity taken
```

Rejected placements go back on the queue; the rest of the batch still commits. The
cache can therefore be wrong without ever being *dangerous* — the worst case is a
wasted decision. Afterwards the cached views are overwritten from the rows the
transaction actually wrote, so drift cannot accumulate.

**A second bottleneck I measured and deliberately did not fix.** Every policy scans
the fleet linearly. At 10,000 workers that is 21 µs per decision — about 47,000
placements/sec on one core, far more than a fleet that size produces. Indexing
workers by available capacity would add a structure to keep coherent across every
allocation change, for no benefit at any plausible scale. The benchmark is the
justification for *not* doing the optimization, which I think is the more useful
engineering output.

**A third I did fix, caught on review:** `CancelJob` and the completion path
refreshed a cached worker view by querying *inside* the scheduler's critical
section. That put database latency in the dispatcher's path on every completion.
Reads now happen before the lock is taken (`ebe29bc`).

---

## 8. Testing strategy

The layers exist because each can prove something the others cannot.

| Layer | Proves | Cannot prove |
| --- | --- | --- |
| Unit (68 core + 11 simulator) | Logic in isolation: every state-machine edge, queue ordering, fit, backoff bounds, store rollback | Anything about concurrency or real failure |
| Integration (26) | Real gRPC, real SQLite, real worker processes, real `SIGKILL` | Behaviour at scale |
| Invariant checker (11 self-tests) | The nine properties, against the durable record | That the properties are the right ones |
| Chaos | The properties hold under randomized real faults | Throughput at scale |
| Simulator (11 self-tests) | Scheduling behaviour at 10k workers / 100k jobs | Network, durability, real timing |
| Benchmarks | Cost of one decision or one queue operation | Anything about system behaviour |

**The part I would point at in an interview:** the invariant checker has tests that
make it *fail*. `internal/invariants/invariants_test.go` constructs databases
containing each specific violation — a resurrected terminal state injected into the
audit log, an oversubscribed worker written behind the store's back, two live
attempts on one job — and asserts the checker names that exact invariant. A checker
that has only ever seen valid states is not evidence it would catch an invalid one.

The same reasoning applies to the simulator, which is tested for determinism under
a seed, job conservation, no oversubscription inside the model, and that the failure
path is actually wired up. A simulator with a bug produces convincing wrong numbers,
which is worse than producing none.

**Fault injection reaches inside the worker, not just outside it.** The worker
binary has flags for dropping a fraction of heartbeats, dropping a fraction of lease
renewals, and replaying every completion report. Those reach cases an external
killer cannot: "the request landed but the response was lost" is only reachable from
inside the client.

---

## 9. Bugs found, and what found them

Each of these is a real commit. The pattern worth noticing is that **none of them
were found by reading the code** — each was caught by a test or benchmark written to
check a property, which is the argument for writing those at all.

| # | Bug | Found by | Commit |
| --- | --- | --- | --- |
| 1 | Priority buckets assumed push order matched enqueue order, which recovery violates (rows come back `created_at DESC`) | A queue unit test asserting strict-priority ordering | `0b20a83` |
| 2 | A lost `StartJob` RPC followed by a fast completion hit an illegal `ASSIGNED → SUCCEEDED` attempt transition | A store test exercising `FinishAttempt` | `0b20a83` |
| 3 | A worker shutting down reported `CANCELED`, **permanently failing** jobs that should have been retried elsewhere | The scheduler-restart integration test | `d2be5b6` |
| 4 | The dispatcher incremented a worker's live-attempt count optimistically, so `atlas_jobs_running` could report work never assigned | A flaky integration assertion that turned out to be right | `d2be5b6` |
| 5 | Rebuilding the queue from database rows was quadratic — newest-first insertion made every insert walk its whole bucket | The queue microbenchmark, showing pop cost growing with depth when the design says it must not | `b2550e7` |
| 6 | The dispatcher had no bound on how far it scanned past an unplaceable job | Simulator runs taking 45 s when they should take 2 | `b2550e7` |
| 7 | Database reads ran under the scheduler mutex, putting query latency in the dispatcher's path | Code review | `ebe29bc` |

### The one worth telling in detail: bug #3

**Symptom.** Three integration tests failed at once: in-flight jobs ended `FAILED`
with the message `execution canceled` after a scheduler restart.

**First instinct — wrong.** It looked like a test-harness problem. The harness was
tearing the whole cluster down (workers included) rather than modelling a scheduler
crash with the fleet surviving.

**What was actually there.** The harness *was* wrong, and fixing it uncovered a
genuine product bug underneath. When a worker shuts down it cancels its in-flight
executions. The executor only knows its context was cancelled, so it reported
`FailureCanceled`. But `CANCELED` is non-retryable by design — it means an operator
or client cancelled the job. So a worker restarting during a rolling deploy would
*permanently fail* every job it happened to be running, with a full attempt budget
remaining.

**Why it is a good bug.** The executor genuinely cannot distinguish "the scheduler
cancelled this job" from "my own process is shutting down" — both are a cancelled
context. The information exists one layer up, in the worker, which knows why it tore
the execution down.

**The fix.** Each execution records an abandonment class when it is torn down.
`WORKER_LOST` (retryable elsewhere) for shutdown or a lost lease; `CANCELED` only
when the scheduler actually said so. The executor's `CANCELED` result is then
overridden by the recorded class:

```go
// The executor only knows its context was canceled, not why. The worker
// does, and the difference decides whether the job is retried.
if res.FailureClass == state.FailureCanceled {
    if class := ex.abandonClass(); class != "" {
        res.FailureClass = class
    }
}
```

**Takeaway.** A failure classification is only as good as the layer that has enough
context to assign it. Pushing the decision down to the executor looked like clean
layering and was actually an information-destroying boundary.

---

## 10. What I would do differently or next

**Differently, with hindsight:**

- **Write the invariant checker earlier.** It was built after the scheduler, and
  three of the seven bugs would have been caught sooner if every test had ended with
  an invariant sweep from day one.
- **Reconsider the denormalized allocation column.** It exists for placement speed
  and is kept honest by invariant I3 — but I3 exists *because* of it. Deriving
  allocation from live attempts would have removed a whole class of bug at the cost
  of one aggregate query per decision, and the benchmarks later showed decisions
  were nowhere near the bottleneck. I would measure first next time.
- **Make the chaos harness bias toward busy workers.** The first campaign injected
  59 faults and lost zero attempts, because best-fit had packed the work onto two
  machines and random kills kept missing them. Raising concurrency fixed it, but a
  harness that cannot reliably hit live work is a harness that proves little.

**Next, in order of value** (expanded in the README):

1. Verify the Docker executor against a live daemon — it has never actually run.
2. Leader election for HA, with consensus isolated to the leadership lease only.
3. Gang scheduling (all-or-nothing placement of N workers in one transaction).
4. Oversubscription with preemption and a non-budget-consuming `PREEMPTED` class.
5. Server-streaming assignment instead of long-poll, to cut dispatch latency and
   push cancellations immediately.

---

## 11. Interview question bank

Questions a reviewer is likely to ask, with the points worth having ready.

<details>
<summary><b>1. Why at-least-once and not exactly-once?</b></summary>

<br/>

- The scheduler cannot distinguish "never ran" from "ran, acknowledgement lost".
  Both present as a lease expiring with no completion.
- Exactly-once would require committing the result in the same transaction as the
  side effect. The side effect is an arbitrary user process; that transaction does
  not exist.
- What Atlas provides instead: idempotent submission, attempt-scoped leases, stale
  write rejection, idempotent completion, bounded retries.
- The responsibility that remains with the workload: `ATLAS_JOB_ID` is stable across
  retries and is the natural idempotency key for whatever it writes.
- Atlas *measures* the duplicates (16 in the flagship campaign) rather than claiming
  zero — and that count is a lower bound, since a worker that dies without reporting
  leaves no evidence.
</details>

<details>
<summary><b>2. Why both heartbeats and leases? Isn't one enough?</b></summary>

<br/>

- They answer different questions. Heartbeat: *is the process alive?* Lease: *does
  this worker still hold authority over this specific attempt?*
- A worker can be alive and heartbeating while making no progress on one job —
  wedged executor, full disk, one deadlocked goroutine. Heartbeats say nothing about
  that.
- A worker can be partitioned in one direction only: receiving acknowledgements,
  unable to send results.
- Losing a worker is strong evidence about *all* its attempts at once, so worker
  death is the fast path; lease expiry is the backstop for what heartbeats cannot
  see.
- Two tests isolate each path: `TestSuspectWorkerKeepsItsWork` (drops 100% of
  heartbeats, keeps renewing, finishes on attempt 1) and
  `TestLeaseExpiryReclaimsAStuckWorker` (heartbeats fine, drops 100% of renewals,
  loses that one attempt while staying `HEALTHY`).
</details>

<details>
<summary><b>3. Why two heartbeat thresholds (SUSPECT and DEAD)?</b></summary>

<br/>

- A single missed heartbeat is common and means almost nothing — a GC pause, a
  scheduling hiccup, a slow network moment.
- `SUSPECT` stops the bleeding: no *new* work goes to the worker, but it keeps what
  it has. No reclamation cost, no duplicate execution.
- `DEAD` is the expensive action, taken only on sustained silence.
- Collapsing them would make every GC pause produce a round of duplicate
  executions.
- Defaults: `SuspectAfter` 6 s (≈3 heartbeats), `DeadAfter` 15 s, matching the
  lease TTL. The ratios are what matter; deployments on slower networks scale all
  three.
</details>

<details>
<summary><b>4. Walk me through the stale-attempt scenario.</b></summary>

<br/>

Use the timeline from §4. Key points:

- Every worker RPC carries `(job_id, attempt_id, lease_id)`.
- `validateRef` checks, inside the transaction: attempt exists, belongs to the job,
  lease id matches, and it is still the job's `current_attempt_id`.
- The subtlety is `LOST` vs other terminal states. `LOST` means Atlas reclaimed it
  and does not know what happened → stale, reject. `SUCCEEDED`/`FAILED`/`CANCELED`
  are outcomes this caller reported → idempotent replay, return the recorded result.
- The rejection is not silent: it increments a counter and, for a late success,
  records a durable duplicate-execution marker.
- The reverse path also exists: heartbeat responses carry `cancel_attempt_ids` so
  the worker stops as soon as the scheduler notices.
</details>

<details>
<summary><b>5. How do you guarantee a worker is never oversubscribed?</b></summary>

<br/>

- `SaveWorker` rejects any write where `allocated > capacity` or `allocated < 0`.
  Enforcement is in the data layer, so no caller can violate it — the transaction
  simply does not commit.
- Reservation and attempt creation happen in the *same* transaction, so there is no
  window where one exists without the other.
- Release happens in `FinishAttempt`, which transitions the attempt to a terminal
  state *and* frees capacity atomically. Terminal states are absorbing, so a second
  call fails the transition and the release cannot run twice.
- The in-memory view is a cache; every placement is re-checked against rows inside
  the transaction.
- Invariant I2 (within capacity, non-negative) and I3 (allocation equals the sum of
  live attempts) are checked mechanically after every test and chaos run.
- Recovery recomputes allocation from live attempts, which repairs drift by
  construction — `TestRecoveryRebuildsAllocationFromLiveAttempts` deliberately
  corrupts the column and asserts it is fixed.
</details>

<details>
<summary><b>6. Why is the queue a map of deques instead of a heap?</b></summary>

<br/>

- Effective priority under aging is `base + rate × wait`, which changes continuously
  for every queued job. A heap's keys would mutate underneath it and the heap
  property would break silently.
- Within a fixed base priority, all the bucketed orderings are monotone in wait
  time, so the bucket's head is always its best job. The global best is the max over
  bucket heads: O(distinct priorities), independent of depth.
- Measured flat: 159 ns at depth 100 and 166 ns at depth 100,000. EDF breaks monotonicity, so
  it uses a real heap and shows O(log n) — 176 ns to 442 ns.
- Gotcha worth mentioning: insertion must preserve bucket order, and recovery loads
  rows newest-first. Getting that wrong made the rebuild quadratic, which the
  benchmark caught.
</details>

<details>
<summary><b>7. What happens when the scheduler crashes mid-assignment?</b></summary>

<br/>

- Nothing partial can exist: the attempt row, the job transition, and the capacity
  reservation are one transaction. Either all of it is durable or none is.
- On restart: every worker is marked `SUSPECT` (heartbeat age unknown, connections
  gone); allocation is recomputed from live attempts; leases that lapsed during the
  outage are reclaimed; the queue is rebuilt from `QUEUED` rows preserving
  `enqueued_at` so accrued priority is not reset.
- Leases are deliberately **not** extended for downtime. A worker that could not
  renew has lost its authority — the scheduler could not observe it, so it assumes
  nothing. The cost is some duplicate execution after a long outage, which is
  exactly what at-least-once permits.
- Two tests: restart with the fleet surviving, and restart with the fleet gone and
  downtime exceeding the lease TTL.
</details>

<details>
<summary><b>8. Why is best-fit worse than least-loaded on your heterogeneous fleet?</b></summary>

<br/>

- Measured: 89.2 s vs 12.1 s mean wait, 7×, on fleets with *identical* total
  capacity differing only in machine shape. Best-fit is marginally the *best* policy
  on the uniform fleet.
- Mechanism: best-fit minimizes leftover capacity, which makes it spend scarce,
  specifically-shaped resources on jobs that did not need them. A 1-core / 12 GiB
  job "best-fits" a 32-core / 16 GiB compute node because 16 GiB leaves less slack
  than a 128 GiB memory node would — consuming that node's scarce memory and
  blocking the CPU-heavy work it exists for.
- Generalizable: tight packing is only a virtue when the thing you pack into is
  interchangeable. On heterogeneous hardware the quantity to preserve is *capacity
  shape*, not raw leftover.
- Also worth noting: heterogeneity alone cost more than the policy choice did —
  moving the same workload to a differently-shaped fleet of equal capacity
  multiplied mean wait by 5× even under the best policy.
</details>

<details>
<summary><b>9. How do you know your benchmark numbers mean anything?</b></summary>

<br/>

- Three tools, each with a stated scope: microbenchmarks measure one function,
  the simulator measures scheduling behaviour at scale, the chaos harness measures
  real processes. `docs/BENCHMARKING.md` says what each can and cannot claim.
- The simulator imports the *real* `ReadyQueue`, `Policy`, and resource model — not
  a reimplementation. What it replaces is the network, the database, and real time.
- Controlled comparison: one variable changes per experiment, everything else
  including the seed is fixed. The fragmentation experiment holds total capacity
  *equal* so only shape differs.
- Load is normalized against a computed sustainable rate, because policy quality is
  invisible on an idle cluster and invisible again under a permanently saturated
  queue.
- Eight named limitations are listed, including that these are single-machine,
  single-run numbers. The differences highlighted are factors, not percentages.
- The simulator has tests for determinism, conservation, and that the failure path
  is wired up.
</details>

<details>
<summary><b>10. Why SQLite rather than Postgres?</b></summary>

<br/>

- The control plane is a single process by design, so a client/server database adds
  an operational dependency and a network hop for no isolation benefit.
- `modernc.org/sqlite` is pure Go: `CGO_ENABLED=0` cross-compiles to three platforms
  in CI and the container image needs no system SQLite.
- Configuration matters more than the engine choice: WAL (readers do not block the
  writer), `synchronous=FULL` (a job is acknowledged only after `fsync`), and a
  single pinned write connection — which removes `SQLITE_BUSY` from the failure
  model entirely.
- The limit, stated honestly: it is a single-writer, single-node store. Making the
  control plane HA means replicating it, which is the main argument for Postgres
  later.
</details>

<details>
<summary><b>11. What is the throughput of the system?</b></summary>

<br/>

Answer the question honestly rather than quoting a number that was not measured:

- **Measured:** placement decision cost (21 µs at 10,000 workers, ~47k/sec on one
  core) and queue operation cost (166 ns, flat to depth 100k).
- **Not measured:** end-to-end submissions per second against a real server. That is
  bounded by `fsync` rather than by CPU, which is why placements are batched — 256
  per transaction turns 256 fsyncs into one.
- The chaos campaign ran 3,000 jobs through 10 workers in 2m46s, but that is a
  correctness test with injected faults and deliberately short job durations, not a
  throughput measurement.
- What I would build to answer it properly: a sustained submission load generator
  against a real server, measuring accepted/sec and p99 submit latency as a function
  of batch size and `synchronous` mode.
</details>

<details>
<summary><b>12. How does admission control decide what to reject?</b></summary>

<br/>

- Four limits: global queue depth, per-client in-flight jobs, per-job request size,
  and "could any worker ever run this even when idle".
- The last one matters most conceptually: a job larger than every machine would
  otherwise sit in the queue forever, occupying a slot and misrepresenting demand.
  It is rejected at submit with `FAILED_PRECONDITION`.
- Ordering is deliberate: **deduplication runs before admission control**, so a
  client retrying an uncertain submission gets the same answer it would have got
  originally, even if the queue filled meanwhile.
- Error codes are part of the contract: `RESOURCE_EXHAUSTED` means back off and
  retry, `FAILED_PRECONDITION` means stop, your assumption is wrong.
- Measured impact: at 2× capacity, an unbounded queue grew to 10,178 jobs with p99
  wait at 10.5 minutes while every submission still returned success. Bounded at
  500, p99 stayed flat at ~34 s across an 8× load range.
</details>

<details>
<summary><b>13. Walk me through your concurrency model. Where are the races?</b></summary>

<br/>

- Three concurrent things: gRPC handlers (one goroutine per call), the dispatch loop
  (woken by a capacity-1 channel so bursts collapse into one sweep), the reconcile
  loop (250 ms ticker).
- One scheduler mutex over shared in-memory state, **never held across a database
  transaction**. Each worker has a mailbox lock for undelivered assignments; lock
  order is scheduler-then-mailbox, never the reverse.
- The interesting race is between the dispatcher's cached fleet view and concurrent
  completions. It is resolved by *allowing* the race and re-validating inside the
  transaction, rather than by locking across I/O. Worst case is a wasted decision.
- A real bug here: database reads originally ran inside the critical section, so
  every completion put query latency in the dispatcher's path. Fixed by reading
  before taking the lock.
- Everything runs under `-race` in CI, including the integration suite with real
  processes.
</details>

<details>
<summary><b>14. How would you add high availability?</b></summary>

<br/>

- Do **not** rewrite the write path around a replicated log. Isolate consensus to
  one concern: the leadership lease.
- Shape: an embedded consensus library holds leadership only; the elected leader
  runs exactly the scheduler that exists today; the SQLite store is replicated by
  log shipping to standbys.
- Fencing is the hard part, and Atlas already has the primitive: a standby that
  takes over must be certain the old leader cannot still be writing. The same
  reasoning as attempt leases, applied one level up — the leadership lease must
  expire before the new leader may act.
- What stays unchanged: all nine invariants, the lease model, the recovery path.
  That is the argument for the isolation — HA becomes additive rather than invasive.
- What gets harder: the duplicate-execution window grows, because a failover is a
  scheduler outage with a shorter but nonzero duration.
</details>

<details>
<summary><b>15. What is the weakest part of this project?</b></summary>

<br/>

Answer directly; the willingness to say it is the point.

- **The Docker executor has never run.** It is implemented and documented but the
  development environment had no Docker daemon. Only the process executor is
  evidence of anything. This is stated in the README, the benchmarking limitations,
  and the PR.
- **No end-to-end throughput measurement.** Decision cost and queue cost are
  measured; submissions/sec against a real server is not.
- **Single-machine, single-run benchmarks.** No repeated trials, no confidence
  intervals. The headline differences are large enough that noise does not explain
  them, but small differences in the tables should not be read as real.
- **The chaos harness never kills the last worker**, because a cluster with no
  capacity cannot make progress and the run would measure a timeout rather than
  recovery. That is a limitation of the harness, stated in `docs/FAILURE_MODEL.md`.
- **No authentication or transport security.** `client_id` is a quota bucket, not an
  identity.
</details>

<details>
<summary><b>Bonus: what does your audit log actually buy you?</b></summary>

<br/>

- Invariant I1 says terminal states are absorbing. Checking only the *final* rows
  would miss a job that went `SUCCEEDED → RUNNING → SUCCEEDED`, which is exactly the
  signature of a stale write winning.
- The `transitions` table is append-only and records every state change with its
  reason, so the checker verifies the property over the whole history, not a
  snapshot. The flagship campaign checked 24,484 transitions.
- It also makes debugging a distributed failure tractable: one query returns the
  complete causal story of a job across every worker that touched it.
- `TestDetectsResurrectedTerminalState` injects an illegal transition directly into
  the log and asserts the checker catches it.
</details>

---

## 12. Numbers worth remembering

| | |
| --- | --- |
| Chaos campaign | 50 faults (24 `SIGKILL`, 17 `SIGSTOP`, 9 scheduler restarts), 3,000/3,000 jobs succeeded, 9/9 invariants held, 24,484 transitions audited |
| Observed duplicate executions | 16 — published, not hidden |
| Recovery latency | p50 64 ms, p99 159 ms from reclamation to reassignment (detection bounded separately by the 4 s threshold) |
| Placement policy impact | 4.5× lower mean wait, 3.5× lower p99 (least-loaded vs round-robin at 90% load) |
| Fragmentation | 7× difference between best and worst policy on fleets of *identical* total capacity |
| Decision cost | 53 ns @ 10 workers → 21 µs @ 10,000; ~47k decisions/sec on one core |
| Queue pop | 159 ns at depth 100, 166 ns at depth 100,000 — flat |
| Admission control | p99 wait flat at ~34 s across an 8× overload range, vs 18 minutes unbounded |
| Tests | 105 total: 68 core unit + 11 simulator self-tests + 26 integration. The 68 include 11 that make the invariant checker fail. All race-clean. |
| Code | 13,628 lines of Go excluding generated protobuf; 3,788 of it tests |

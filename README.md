# Atlas

A fault-tolerant distributed compute scheduler: it accepts resource-constrained
jobs, places them across a worker fleet, tracks every execution with a lease, and
recovers automatically when machines, processes, or networks fail.

> **Atlas does not promise exactly-once execution.** It provides at-least-once
> execution with attempt-scoped leases and idempotent control-plane operations. A
> single job may run more than once; the system is built so that this is safe,
> bounded, observable, and *measured* rather than denied.
> See [`docs/SEMANTICS.md`](docs/SEMANTICS.md).

```
$ atlas submit --cpu 1 --memory 256m -- echo "hello atlas"
job_738743db435dd3cb    JOB_STATE_QUEUED
QUEUED
SUCCEEDED

job              job_738743db435dd3cb
state            SUCCEEDED
request          1 cpu, 256MiB memory
attempts         1 of 3
exit code        0

#  ATTEMPT ID            WORKER    STATE      EXIT  DURATION  MESSAGE
1  att_7cab8056ca732af3  worker-a  SUCCEEDED  0     4ms

--- attempt 1 (att_7cab8056ca732af3) output ---
hello atlas
```

```
$ atlas-chaos --workers 10 --jobs 3000 --duration 150s --restart-scheduler \
              --duplicate-rpc-rate 0.2 --heartbeat-drop-rate 0.1

Faults injected
  kill-worker               24
  pause-worker              17
  restart-scheduler         9

Outcome
  jobs in store             3000
  succeeded                 3000
  failed after retry limit  0

Execution attempts
  attempts total            3083
  lost (lease reclaimed)    83
  retries                   83
  duplicate executions      16

Invariants (docs/SEMANTICS.md §7), checked against 24484 audited state changes
  I1  terminal states were never left                      held
  I2  no worker was oversubscribed or went negative        held
  ...
  I9  job and attempt states agree                         held

VERDICT: PASS - every documented invariant held under fault injection
```

## Contents

- [Architecture](#architecture)
- [Execution semantics](#execution-semantics)
- [Job state machine](#job-state-machine)
- [Scheduling and the resource model](#scheduling-and-the-resource-model)
- [Leases and failure recovery](#leases-and-failure-recovery)
- [Persistence](#persistence)
- [Idempotency](#idempotency)
- [Backpressure](#backpressure)
- [Observability](#observability)
- [Chaos testing](#chaos-testing)
- [Results](#results)
- [Running it](#running-it)
- [Limitations](#limitations)

## Architecture

```
   CLI ──gRPC──▶  ┌─────────── CONTROL PLANE (one atlas-server) ───────────┐
                  │  API: admission, idempotency, error mapping            │
                  │  Dispatch loop: policy, backfill window, batched commit│
                  │  Reconcile loop: lease expiry, worker health, resync   │
                  │  SQLite (WAL, synchronous=FULL)                        │
                  │    jobs · attempts · workers · transitions (audit log) │
                  └───────────────────▲───────────────────────────────────-┘
                                      │ workers always dial in
                  ┌───────────────────┴────────────────┐
           ┌──────────────┐                   ┌──────────────┐
           │ atlas-worker │                   │ atlas-worker │
           │   8 CPU      │                   │   4 CPU      │
           │   16 GiB     │                   │   8 GiB      │
           └──────┬───────┘                   └──────┬───────┘
                  ▼                                  ▼
          process / container               process / container
```

Two directions matter. **Every connection is opened by a worker**, so the scheduler
never dials out and workers need no inbound reachability. **The scheduler still owns
placement** — workers long-poll for assignments the dispatcher has already decided
on and already committed, which keeps the placement policy centralized and
comparable instead of being an emergent property of who polled first.

Full detail, including the concurrency model and where the in-memory caches can
drift: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Execution semantics

The guarantee and its justification are in [`docs/SEMANTICS.md`](docs/SEMANTICS.md),
which was written before the scheduler was.

The short version. Consider a worker that finishes a job and then dies before the
scheduler hears about it. At the moment the lease expires, the scheduler holds
exactly one fact: it has not received a completion. It cannot distinguish "the work
never happened" from "the work happened and the acknowledgement was lost". Any
system claiming otherwise is either doing the commit inside the same transaction as
the side effect — which Atlas cannot, because the side effect is an arbitrary user
process — or is wrong.

So Atlas retries, and says that it retries. What it provides instead:

| Property | Guarantee |
| --- | --- |
| Job submission | Idempotent, keyed by a client-supplied `idempotency_key` |
| Job execution | At-least-once, bounded by `max_attempts` |
| Control-plane state | Serializable, in a single SQLite database with one writer |
| Attempt authority | At most one attempt per job holds a valid lease at any time |
| Stale results | A result from a non-current attempt can never mutate job state |
| Durability | A job acknowledged to a client survives scheduler restart |
| Completion reporting | Idempotent; a replay returns the recorded outcome |

Every execution receives `ATLAS_JOB_ID` (stable across retries) and
`ATLAS_ATTEMPT_ID` (unique per physical execution) in its environment, so a workload
with external side effects has what it needs to make them idempotent.

## Job state machine

```
                       SUBMITTED
                           │
                           ▼
            ┌──────────▶ QUEUED ◀──────────────────┐
            │              │                       │
            │              ▼                       │
   retry    │          ASSIGNED                    │  lease expired,
   with     │              │                       │  worker declared dead,
   backoff  │              ▼                       │  or attempt lost
            │           RUNNING ───────────────────┘
            │            ╱ │ ╲
            │           ╱  │  ╲
            └──── SUCCEEDED FAILED CANCELED      (terminal, all three)
```

Every transition goes through `internal/state`, which rejects illegal edges and
appends to an audit table. Nothing in the codebase assigns a state directly. That is
what makes "terminal states are absorbing" checkable over an entire run rather than
only at its end.

Attempts have their own state machine, and `LOST` is deliberately distinct from
`FAILED`: `FAILED` means Atlas knows the execution finished badly, `LOST` means
Atlas does not know what happened. They are retried under different policies.

## Scheduling and the resource model

Jobs declare CPU in millicores and memory in bytes. A worker fits a job only if both
dimensions fit and the worker is `HEALTHY` and not draining.

**Placement policies** (`--policy`):

- `round-robin` — the deliberately stupid baseline, and a useful control
- `least-loaded` — the worker with the most free capacity, averaged across both
  dimensions
- `best-fit` — the worker with the least capacity left over afterwards

**Queue orderings** (`--ordering`):

- `fifo` — oldest first, priority ignored
- `priority` — strict priority, which can starve low-priority work indefinitely
- `priority-aging` — effective priority is `base + rate × wait`, capped
- `edf` — earliest deadline first

The queue is a map of per-priority deques sorted by enqueue time, not one heap, and
that choice is load-bearing. For FIFO, strict priority, and aging, the best job in a
bucket is always the bucket's oldest, so the global best is found by comparing
bucket heads — O(distinct priorities), independent of queue depth. A heap cannot do
this because aging changes every key continuously. Measured: pop cost is flat at
~160–210 ns from depth 100 to depth 100,000.

A sweep that cannot place a job moves on to the next one rather than blocking behind
it, up to `--dispatch-scan` candidates. That backfill window is explicit because
scanning a hundred thousand queued jobs every 25 ms to discover nothing fits is
worse than waiting for the next sweep.

## Leases and failure recovery

A lease is not a heartbeat, and the distinction is the heart of the design.

- A **heartbeat** answers: *is this worker process alive?*
- A **lease** answers: *does this worker currently hold authority to execute this
  specific attempt?*

Those are different questions. A worker can be alive and heartbeating while making
no progress on one job. A worker can be healthy but partitioned in one direction.

```
HEALTHY ──missed 3 heartbeats──▶ SUSPECT ──sustained silence──▶ DEAD
   ▲                                │                             │
   └─────── a heartbeat ────────────┘                             ▼
                                              every attempt on it reclaimed at once

attempt assigned ──▶ renewed every LeaseTTL/4 ──▶ not renewed for LeaseTTL ──▶ LOST
```

Two heartbeat thresholds rather than one, because a single missed heartbeat means
almost nothing. `SUSPECT` stops the bleeding — no new work — without paying for
reclamation. Collapsing the two would turn every GC pause into a round of duplicate
executions.

### Stale attempt protection

```
t0  worker A is assigned job J, attempt 1, lease L1
t1  A is partitioned; it keeps running
t2  L1 expires; attempt 1 → LOST, J requeued
t3  J assigned to worker B as attempt 2, lease L2
t4  A's partition heals
t5  A reports "J completed successfully"
```

At t5 that report must not become J's outcome — B may still be running and may
produce a different one. Every worker RPC carries `(job_id, attempt_id, lease_id)`,
and the scheduler validates inside the transaction that the attempt exists, that the
lease matches, and that it is still the job's current attempt. A's report is
rejected with `FAILED_PRECONDITION` and recorded as an observed duplicate execution —
counted, not hidden.

Which faults are handled, how fast, and which are not:
[`docs/FAILURE_MODEL.md`](docs/FAILURE_MODEL.md).

## Persistence

SQLite in WAL mode with `synchronous=FULL`, a single pinned writer connection, and a
separate read pool. A job is acknowledged to a client only after its row has been
`fsync`'d.

The store exposes *transactions*, not operations. Callers compose their reads and
writes inside `Update(func(*Tx) error)`, so assigning a job — create the attempt,
move the job to `ASSIGNED`, reserve the worker's capacity — is one atomic fact
rather than three hopeful ones. Two correctness rules are enforced by the data layer
rather than by convention:

- State changes only happen through `TransitionJob` / `TransitionAttempt`, which
  validate the edge and append to the audit log.
- `SaveWorker` refuses negative or oversubscribed allocations, so no scheduler bug
  can oversubscribe a worker; the transaction simply fails.

On restart the scheduler marks every worker `SUSPECT`, recomputes each worker's
allocation from the attempts actually live on it, reclaims every lease that lapsed
while it was down, and rebuilds the queue preserving `enqueued_at` so a restart does
not reset accrued priority.

## Idempotency

**Submission** is keyed on `idempotency_key`, which is `UNIQUE` in the database.
Resubmitting returns the same `job_id` with `deduplicated=true`. Reusing a key with
materially different parameters returns `ALREADY_EXISTS` rather than silently
returning the old job — otherwise a client would believe it had submitted work Atlas
never saw.

**Completion** is idempotent per attempt. `StartJob`, `RenewLease`, `CompleteJob`,
and `FailJob` can all be safely retried, because the loss of a *response* is
indistinguishable from the loss of a *request*. A replay returns the recorded
outcome rather than applying a second state change.

The deduplication lookup deliberately happens *before* admission control: a client
retrying a submission it is unsure about must get the same answer it would have got
the first time, even if the queue filled up in between.

## Backpressure

Admission control bounds the queue depth, the in-flight jobs per client, and the
per-job request size, and rejects jobs no registered worker could ever run.
Rejection is `RESOURCE_EXHAUSTED`, which tells the caller to back off, rather than
an unbounded queue, which tells it nothing.

The cost of not having it is measured in [`docs/RESULTS.md`](docs/RESULTS.md):
at 2× capacity, an unbounded queue grew to 10,178 jobs and p99 wait reached 10.5
minutes while still reporting success to every caller.

## Observability

Prometheus metrics on `/metrics`, covering submissions, completions, rejections,
attempts, retries, lease expirations, stale rejections, duplicate executions,
idempotent replays, worker counts by state, queue depth split by backoff
eligibility, schedule latency, job wait and runtime, fleet allocation and
utilization, and recovery latency.

Health endpoints distinguish the two questions that are usually conflated: `/live`
asks whether the process is running, `/ready` asks whether it can safely accept work
(it fails if the database does not answer). The standard gRPC health protocol is
served as well.

Logs are events with identifiers, never sentences:

```
event=job_assigned   job_id=job_42 attempt_id=att_9f attempt=2 worker_id=w3 lease_id=lse_11
event=lease_expired  job_id=job_42 attempt_id=att_9f worker_id=w3 late_by_ms=812
event=job_requeued   job_id=job_42 attempt_id=att_9f failure_class=WORKER_LOST backoff_ms=340
```

## Chaos testing

`atlas-chaos` starts a real scheduler and real workers as child processes and
injects faults the way they happen in production: `SIGKILL`, `SIGSTOP` (a worker
that is alive, holds its leases, and answers nothing), scheduler restarts, dropped
heartbeats, dropped lease renewals, and replayed RPCs.

Afterwards it reads the SQLite database the run produced and checks all nine
documented invariants, exiting non-zero on any violation so it works as a CI gate
rather than only as a demo:

1. Terminal states are absorbing — verified against the *audit log*, not the final
   rows, so a job that went `SUCCEEDED` then `RUNNING` then `SUCCEEDED` is caught.
2. No worker is oversubscribed, and no allocation goes negative.
3. Each worker's allocation equals the sum of its live attempts' requests.
4. At most one live attempt per job, and the job points at it.
5. `attempt_count ≤ max_attempts`, and the attempt rows agree with the counter.
6. Every job acknowledged to a client is still in the store.
7. Idempotency keys are unique.
8. No stale attempt decided a job's outcome.
9. Job state and current attempt state agree.

The checker has its own tests, which build a database containing each specific
violation and assert that it is named. An invariant checker that has only ever seen
valid states is not evidence that it would catch an invalid one.

## Results

Full tables and methodology: [`docs/RESULTS.md`](docs/RESULTS.md) and
[`docs/BENCHMARKING.md`](docs/BENCHMARKING.md). Four findings worth the summary.

**Fleet heterogeneity costs far more than policy choice, and best-fit is the worst
answer to it.** Two fleets with *identical* total capacity — 384 cores, 928 GiB, 20
machines — differ only in machine shape:

| Fleet | Policy | Mean wait | p99 wait | Stranded CPU |
| --- | --- | --- | --- | --- |
| uniform | best-fit | 2.48s | 51.7s | 8.3% |
| uniform | least-loaded | 2.65s | 61.0s | 10.3% |
| heterogeneous | least-loaded | 12.1s | 6.7m | 18.1% |
| heterogeneous | round-robin | 24.8s | 12.4m | 22.8% |
| heterogeneous | **best-fit** | **89.2s** | **19.6m** | **27.9%** |

Best-fit is 7× worse than least-loaded on the heterogeneous fleet while being
marginally the best on the uniform one. Minimizing leftover capacity spends the
scarce, specifically-shaped resources on jobs that did not need them — a 1-core /
12 GiB job "best-fits" a 32-core / 16 GiB compute node, consuming that node's scarce
memory and blocking the CPU-heavy work it exists for.

**An aging cap can silently restore the starvation it was added to prevent.**
Low-priority jobs under a saturated cluster:

| Ordering | urgent mean wait | batch mean wait |
| --- | --- | --- |
| fifo | 6.4m | 6.4m |
| strict priority | 3.2m | 13.6m |
| aging 1/s, cap 1000 | 5.2m | 9.1m |
| aging 10/s, cap 1000 | 3.8m | 12.3m |
| aging 10/s, no cap | 6.3m | 6.7m |

Aging at 10/s *with a cap* behaves almost like strict priority, because once every
queued job has saturated the cap the only thing separating the classes is their base
priority again. The same rate uncapped collapses to FIFO. The cap is not a safety
rail; it is the dial.

**Admission control converts an invisible failure into a visible one.** At 2× the
sustainable arrival rate:

| Admission | Accepted | Rejected | Peak queue | p99 wait |
| --- | --- | --- | --- | --- |
| none | 20,000 | 0 | 10,178 | 10.5m |
| queue ≤ 500 | 10,296 | 9,704 | 500 | 34.7s |

Without it, every submission succeeds and the cost lands entirely on latency, which
the caller cannot see until it is already enormous.

**The O(workers) placement scan is fine to ~10,000 machines.** Least-loaded and
best-fit are strictly linear: 53 ns at 10 workers, 240 ns at 100, 2.1 µs at 1,000,
21 µs at 10,000 — still ~47,000 decisions/sec on one core. Round-robin is flat at
35 ns whenever *some* worker fits, and becomes the most expensive policy when none
does.

**The chaos campaign's most important number is not zero.** Across 50 injected
faults, 3,000 of 3,000 jobs succeeded and all nine invariants held — but sixteen
times a reclaimed worker reported afterwards that it had in fact finished its job.
Those jobs really did run twice. Atlas rejected the late reports so none of them
changed an outcome, and then published the count. A chaos report claiming zero
duplicates under at-least-once semantics is either measuring nothing or lying.
Chaos runs are deliberately not bit-reproducible: between two runs of the identical
command the fault counts moved by 30–50%, while "every job finished" and "zero
invariant violations" did not move at all. Those are the two rows that are supposed
to be properties rather than measurements.

## Running it

```bash
make build

# Control plane
./bin/atlas-server --db atlas.db --listen :50051 --http :9090

# Workers
./bin/atlas-worker --scheduler localhost:50051 --id w1 --cpu 8 --memory 16GB
./bin/atlas-worker --scheduler localhost:50051 --id w2 --cpu 4 --memory 8GB

# Client
./bin/atlas submit --cpu 1 --memory 256m --wait -- echo "hello atlas"
./bin/atlas list
./bin/atlas workers
./bin/atlas drain w2
./bin/atlas status
```

Or `docker compose -f deployments/docker-compose.yml up` for a scheduler,
three heterogeneous workers, and Prometheus.

```bash
make test             # unit tests
make test-race        # under the race detector
make test-integration # real gRPC, real SQLite, real worker kills
make chaos            # a short fault-injection campaign
make experiments      # every simulator experiment, into ./results
make bench            # scheduler microbenchmarks
```

## Limitations

Stated plainly, because a system's limits are part of its specification.

- **Not highly available.** One scheduler process, no consensus, no replication. If
  it dies the control plane is unavailable until it restarts; running jobs keep
  running and are reconciled afterwards. Atlas is crash-*recoverable*, not
  fault-*tolerant to scheduler loss*. It does not survive loss of its disk.
- **No Raft, deliberately.** Consensus is the reflex answer to "distributed
  scheduler" and it would make this project worse: the hard parts here are lease
  ownership, failure detection, resource accounting, idempotency, and proving the
  result, and a hand-rolled consensus implementation would add a large surface of
  subtle bugs without improving any of them.
- **The Docker executor is unverified.** It is implemented and documented, but the
  development environment had no Docker daemon, so only the process executor has
  actually been run.
- **No authentication, authorization, or TLS.** `client_id` is a quota bucket, not
  an identity. Atlas assumes a trusted network.
- **No job dependencies**, no DAGs, no workflows, no autoscaling, no GPU support.
- **Duplicate execution counts are a lower bound.** Atlas counts the duplicates it
  observes — a reclaimed worker reporting success afterwards. A worker that dies
  mid-job without reporting leaves no evidence either way, and nothing can recover
  that evidence later.
- **Benchmarks are single-machine, single-run.** The differences reported above are
  large enough that noise does not explain them; the small ones in the full tables
  should not be read as real.

# Atlas Architecture

## The shape of the system

```
                              ATLAS
                            ┌───────┐
                            │  CLI  │
                            └───┬───┘
                                │ gRPC: SubmitJob, GetJob, ListJobs,
                                │       CancelJob, ListWorkers, DrainWorker
                                ▼
         ┌──────────────────────────────────────────────┐
         │                CONTROL PLANE                 │
         │             (one atlas-server)               │
         │                                              │
         │  ┌────────────┐      ┌────────────────────┐  │
         │  │ gRPC API   │      │   Dispatch loop    │  │
         │  │            │      │  placement policy  │  │
         │  │ admission  │─────▶│  backfill window   │  │
         │  │ idempotency│      │  batched commit    │  │
         │  └────────────┘      └────────────────────┘  │
         │         │                      │             │
         │         │            ┌────────────────────┐  │
         │         │            │  Reconcile loop    │  │
         │         │            │  lease expiry      │  │
         │         │            │  worker health     │  │
         │         │            │  queue resync      │  │
         │         │            └────────────────────┘  │
         │         ▼                      ▼             │
         │  ┌────────────────────────────────────────┐  │
         │  │       SQLite (WAL, synchronous=FULL)   │  │
         │  │  jobs · attempts · workers · transitions│ │
         │  └────────────────────────────────────────┘  │
         └──────────────────────────────────────────────┘
                     ▲                      ▲
                     │ gRPC: Register, Heartbeat, AcquireJob,
                     │       StartJob, RenewLease, CompleteJob, FailJob
          ┌──────────┴───────────┐         │
          │                      │         │
   ┌──────────────┐       ┌──────────────┐ │
   │  atlas-worker│       │  atlas-worker│ │
   │   8 CPU      │       │   4 CPU      │ │
   │   16 GiB     │       │   8 GiB      │ │
   │              │       │              │ │
   │  heartbeat   │       │  heartbeat   │ │
   │  acquire     │       │  acquire     │ │
   │  renew lease │       │  renew lease │ │
   └──────┬───────┘       └──────┬───────┘ │
          │                      │         │
          ▼                      ▼         │
    EXECUTION PLANE        EXECUTION PLANE │
    process / container   process / container
```

Two directions are worth noticing.

**Every connection is opened by a worker.** The scheduler never dials out. Workers
can sit behind NAT, the control plane needs no service discovery, and a worker that
cannot reach the scheduler simply loses its leases rather than entering a
half-connected state.

**The scheduler still owns placement.** Workers pull, but they pull assignments the
dispatcher has already decided on and already committed. The alternative — workers
choosing their own work — would make the placement policy an emergent property of
whoever polled first, and would make the comparison in `docs/RESULTS.md`
meaningless.

## Request flow

### Submitting a job

```
client ──SubmitJob(idempotency_key, …)──▶ API
                                          │
                                          ▼
                                   BEGIN TRANSACTION
                                     lookup idempotency_key ──▶ found? return it
                                     admission checks
                                     INSERT job (SUBMITTED)
                                     transition SUBMITTED → QUEUED
                                   COMMIT  (fsync)
                                          │
                                          ▼
                                   push onto in-memory queue
                                   wake the dispatcher
                                          │
                                          ▼
client ◀───────── job_id, QUEUED ─────────┘
```

The deduplication lookup happens before admission control, deliberately. A client
retrying a submission it is unsure about must get the same answer it would have got
the first time, even if the queue filled up in between.

### Placing a job

```
dispatcher wakes (submission, completion, worker registered, or 25ms tick)
   │
   ├─ promote jobs whose retry backoff has elapsed
   │
   ├─ loop, up to MaxDispatchBatch placements or MaxDispatchScan candidates:
   │     pop the best job from the ready queue
   │     ask the policy for a worker
   │     reserve against the cached worker view
   │     (no fit?  leave it queued and try the next job — the backfill window)
   │
   └─ ONE transaction for the whole batch:
         for each placement, re-read the job and the worker and re-check:
            job still QUEUED?    worker still schedulable?    does it still fit?
            INSERT attempt (ASSIGNED, lease_id, expires_at)
            job QUEUED → ASSIGNED, attempt_count++
            worker.allocated += request          ← refused if it would oversubscribe
         COMMIT  (one fsync for the whole batch)
   │
   ├─ overwrite cached worker views from the committed rows
   └─ deliver assignments to each worker's mailbox
```

Batching is not a micro-optimization. Every assignment has to be durable before a
worker is told about it, and durability costs an `fsync`. Committing N placements
together turns N fsyncs into one.

The re-validation inside the transaction is what makes the in-memory cache safe. The
policy chose using a snapshot that may be stale; the transaction decides using rows
that are not. A stale cache costs a wasted decision and nothing else.

### Executing and reporting

```
worker ──AcquireJob (long poll)──▶ scheduler        returns committed assignments
worker ──StartJob(job, attempt, lease)──▶           attempt ASSIGNED → RUNNING
worker    …runs the workload…
worker ──RenewLease(…)──▶ every LeaseTTL/4          extends expires_at
worker ──CompleteJob(…)──▶                          attempt → SUCCEEDED, job → SUCCEEDED
                                                    worker.allocated -= request
```

Every one of those worker calls carries `(job_id, attempt_id, lease_id)`. See
`docs/SEMANTICS.md` §4 for why.

## Package layout

| Package | Responsibility |
| --- | --- |
| `internal/state` | Job, attempt, and worker states and the only legal transitions between them. No I/O. |
| `internal/types` | Domain objects shared by everything. No behaviour. |
| `internal/store` | SQLite. Exposes transactions, not operations. Validates every transition, appends the audit log, refuses oversubscription. |
| `internal/scheduler` | Placement, leases, retries, admission, recovery. Knows nothing about protobuf. |
| `internal/api` | gRPC. Converts wire types and maps errors onto status codes. Nothing else. |
| `internal/worker` | The agent: register, heartbeat, acquire, renew, execute, report. |
| `internal/invariants` | Mechanical checker for the properties in `docs/SEMANTICS.md` §7. |
| `internal/metrics`, `internal/logging` | Prometheus collectors and structured logging. |
| `simulator/` | Discrete-event model that reuses the real queue, policies, and resource model. |
| `chaos/` | Fault-injection campaigns against real processes. |

Two boundaries do real work:

`internal/scheduler` does not import protobuf. That is why the simulator can drive
the queue and the policies directly, and why the unit tests need no network.

`internal/store` exposes `Update(func(*Tx) error)` rather than methods like
`AssignJob`. Composing reads and writes at the call site is what lets the scheduler
make "create the attempt, move the job, reserve the capacity" one atomic fact
instead of three hopeful ones.

## Concurrency model

The scheduler runs three things concurrently:

- **gRPC handlers**, one goroutine per call.
- **The dispatch loop**, woken by a one-capacity channel. A pending wakeup already
  covers any number of new events, so a burst of submissions produces one sweep.
- **The reconcile loop**, on a 250ms ticker: worker health, lease expiry, queue
  resync.

Shared state is guarded by a single mutex on the `Scheduler`, held only around
in-memory operations, never across a database transaction. Each worker additionally
has a small mailbox lock for undelivered assignments; the lock order is scheduler
then mailbox, and nothing takes the scheduler lock while holding a mailbox lock.

Writes to SQLite are serialized by a single pinned connection, which removes
`SQLITE_BUSY` from the failure model entirely. Reads go to a separate pool and, under
WAL, do not block the writer.

## Where the in-memory state could drift, and why it does not matter

| Cached state | Reconciled by |
| --- | --- |
| Worker allocation | Overwritten from committed rows after every dispatch batch, lease sweep, and attempt completion. Also recomputed from scratch on recovery. |
| Ready queue | Additively resynced from the `QUEUED` rows every second, so a crash between committing a requeue and pushing it in memory cannot strand a job. |
| Worker health | Derived from heartbeats, which are in memory by design; recovery marks every worker `SUSPECT` and lets a heartbeat promote it. |

The resync is additive only. Removing entries would race with concurrent
submissions, and it is unnecessary: placement re-validates every job against its row
before assigning it.

## What Atlas is not

These are scope decisions, not oversights.

**Not highly available.** One scheduler process, no consensus, no replication. If it
dies the control plane is unavailable until it restarts; running jobs keep running
and are reconciled on restart. Atlas is crash-*recoverable*, not fault-*tolerant to
scheduler loss*.

**Not Raft.** Consensus is the reflex answer to "distributed scheduler" and it would
make this project worse. The hard parts here are lease ownership, failure detection,
resource accounting, idempotency, and proving the result — and a hand-rolled
consensus implementation would add a large surface of subtle bugs without improving
any of them. Leader election over a replicated log is a coherent future phase, not a
prerequisite.

**Not a DAG engine.** No dependencies between jobs, no workflows, no fan-out.

**Not multi-tenant in any security sense.** `client_id` is a quota bucket, not an
identity. There is no authentication, authorization, or transport security; Atlas
assumes a trusted network.

**Not an autoscaler.** Atlas places work on the fleet it has and tells you, through
`atlas_queue_depth` and the rejection counters, when that fleet is too small.

**Not a log store.** Each attempt keeps the last 4 KiB of stdout and stderr, enough
to see why something failed. Anything more belongs in the workload's own logging.

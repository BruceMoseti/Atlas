# Atlas Execution Semantics

This document is normative. It was written before the scheduler was implemented, and
the implementation is expected to conform to it. Where the implementation and this
document disagree, one of the two is a bug.

## 1. The headline guarantee

**Atlas provides at-least-once execution, not exactly-once execution.**

A single logical job may produce more than one physical execution attempt. Atlas does
not, and cannot, prevent this.

The reason is the classic one. Consider:

```
worker starts job
      |
      v
job's process exits 0
      |
      X  worker crashes / network partitions before the scheduler is told
      |
      v
scheduler's lease for that attempt expires
```

At the moment the lease expires, the scheduler holds exactly one fact: it has not
received a completion. It cannot distinguish "the work never happened" from "the work
happened and the acknowledgement was lost". Any system that claims otherwise is either
(a) doing the commit inside the same transaction as the side effect, which Atlas cannot
do because the side effect is an arbitrary user process, or (b) wrong.

Atlas therefore chooses to retry, and tells you that it retries.

### What Atlas provides instead

| Property | Guarantee |
| --- | --- |
| Job submission | Idempotent, keyed by a client-supplied `idempotency_key`. |
| Job execution | At-least-once, bounded by `max_attempts`. |
| Control-plane state | Linearizable per job, via single-writer serialized SQLite transactions. |
| Attempt authority | At most one attempt per job holds a valid lease at any time. |
| Stale results | A result from a non-current attempt can never mutate authoritative job state. |
| Durability | A job that has been acknowledged to a client survives scheduler restart. |
| Completion reporting | Idempotent; replaying a completion returns the recorded outcome. |

### What the user's workload must do

If your job has external side effects (writes to a database, sends an email, charges a
card), the workload itself must be idempotent, or must use an application-level
transaction or idempotency key. Atlas hands you the tools to do this: every execution
receives `ATLAS_JOB_ID`, `ATLAS_ATTEMPT_ID`, and `ATLAS_ATTEMPT_NUMBER` in its
environment. `ATLAS_JOB_ID` is stable across retries and is a natural idempotency key
for downstream systems. `ATLAS_ATTEMPT_ID` is unique per physical execution.

## 2. Job state machine

```
                       SUBMITTED
                           |
                           v
            +----------> QUEUED <------------------+
            |              |                       |
            |              v                       |
            |          ASSIGNED                    |  lease expired, worker
   retry    |              |                       |  declared dead, or
   backoff  |              v                       |  attempt lost
            |           RUNNING ------------------>+
            |            / | \
            |           /  |  \
            |          v   v   v
            +---- SUCCEEDED FAILED CANCELED
                  (terminal, all three)
```

`SUBMITTED` exists only inside the submission transaction: the job row is written, then
validated into `QUEUED`, then the transaction commits. A `SUBMITTED` row is never
visible to a reader, but the state and its transition are modelled explicitly so that a
crash between the two writes cannot leave an un-enqueued job behind.

Formal transition table (`internal/state/state.go` is the single source of truth):

| From | Allowed to |
| --- | --- |
| `SUBMITTED` | `QUEUED`, `CANCELED` |
| `QUEUED` | `ASSIGNED`, `CANCELED`, `FAILED` |
| `ASSIGNED` | `RUNNING`, `QUEUED`, `CANCELED`, `FAILED` |
| `RUNNING` | `SUCCEEDED`, `FAILED`, `CANCELED`, `QUEUED` |
| `SUCCEEDED` | — |
| `FAILED` | — |
| `CANCELED` | — |

`QUEUED -> FAILED` is how retry exhaustion is recorded: the last attempt is lost, the
job returns to the queue logically, and the scheduler immediately observes that
`attempt_count >= max_attempts` and fails it in the same transaction.

### Attempt state machine

An attempt is one physical execution. Attempts are append-only records; they are never
reused.

```
   ASSIGNED ---> RUNNING ---> SUCCEEDED
      |             |
      |             +-------> FAILED
      |             |
      +-------------+-------> LOST      (lease expired / worker declared dead)
      |             |
      +-------------+-------> CANCELED
```

| From | Allowed to |
| --- | --- |
| `ASSIGNED` | `RUNNING`, `FAILED`, `LOST`, `CANCELED` |
| `RUNNING` | `SUCCEEDED`, `FAILED`, `LOST`, `CANCELED` |
| `SUCCEEDED`, `FAILED`, `LOST`, `CANCELED` | — |

`LOST` is distinct from `FAILED` on purpose. `FAILED` means Atlas knows the execution
finished badly. `LOST` means Atlas does not know what happened. They are retried under
different policies (see §6).

## 3. Leases

A lease is not a heartbeat, and the distinction is the heart of Atlas's failure
handling.

- A **heartbeat** answers: *is this worker process probably alive?*
- A **lease** answers: *does this worker currently hold authority to execute this
  specific attempt?*

These are different questions with different answers. A worker can be alive and
heartbeating while being unable to make progress on one particular job, and a worker
can be healthy but partitioned from the scheduler in one direction.

Every assignment creates a lease:

```
lease = (lease_id, attempt_id, job_id, worker_id, expires_at)
```

- The lease is created in the same transaction as the attempt and the job's move to
  `ASSIGNED`.
- The worker must call `RenewLease(job_id, attempt_id, lease_id)` before `expires_at`.
  Renewal extends `expires_at` to `now + lease_ttl`.
- If `expires_at` passes, the scheduler's reconciler declares the attempt `LOST`,
  releases the worker's reserved resources, and requeues the job (subject to retry
  limits).

Once a lease has expired and been reclaimed, it is dead forever. The worker that held
it has no authority, even if it reappears.

### The two reclamation paths

| Trigger | Scope | Latency |
| --- | --- | --- |
| Lease expiry | one attempt | `lease_ttl` after the last renewal |
| Worker declared `DEAD` | every active attempt on that worker | `heartbeat_timeout` after the last heartbeat |

Worker death is the fast path, because losing a worker is strong evidence about all of
its attempts at once. Lease expiry is the backstop that catches the cases heartbeats
cannot see.

## 4. Stale attempt protection

This is the scenario that makes at-least-once safe to operate:

```
t0  worker A is assigned job J, attempt 1, lease L1
t1  worker A is partitioned; it keeps running the job
t2  L1 expires; scheduler marks attempt 1 LOST, requeues J
t3  job J is assigned to worker B as attempt 2, lease L2
t4  worker A's partition heals
t5  worker A reports "job J completed successfully"
```

At `t5`, worker A's report **must not** become the authoritative outcome of J. Worker B
may still be running, and may produce a different result.

Atlas enforces this by making every worker-originated mutation attempt-scoped. Each of
`StartJob`, `RenewLease`, `CompleteJob`, and `FailJob` carries `(job_id, attempt_id,
lease_id)`, and the scheduler validates, inside the transaction:

1. The attempt exists and belongs to the job.
2. `lease_id` matches the lease recorded for that attempt.
3. The attempt is the job's `current_attempt_id`.
4. The attempt is not already terminal (or, if it is, see idempotency below).

If check 3 fails the RPC is rejected with `FAILED_PRECONDITION` and the counter
`atlas_stale_attempt_rejections_total` is incremented. The response instructs the worker
to abandon and kill the execution.

Worker A's late success is recorded on *attempt 1* for observability (we want the
duplicate-execution count to be measurable, see `docs/BENCHMARKING.md`), but it never
touches job J's state.

## 5. Idempotency

### Submission

`SubmitJob` carries an `idempotency_key`. The key is `UNIQUE` in the jobs table.
Re-submitting with the same key returns the existing job — the same `job_id`, the same
`created_at` — and sets `deduplicated = true` in the response. The job's parameters are
*not* updated; the first write wins. If a client reuses a key with materially different
parameters, Atlas returns `ALREADY_EXISTS` rather than silently ignoring the difference.

If a client supplies no key, Atlas generates one. Such a submission is not
deduplicated, and a client-side retry will create a second job. This is documented
rather than hidden.

### Completion

`CompleteJob` and `FailJob` are idempotent per attempt. If the attempt is already
terminal with the same outcome, the scheduler returns success with the recorded result
instead of erroring or re-applying the transition. If it is already terminal with a
*different* outcome, the first outcome wins and the response reports the recorded one.

`StartJob` is idempotent: starting an already-`RUNNING` attempt is a no-op success.
`RenewLease` is naturally idempotent.

This matters because the loss of a *response* is indistinguishable from the loss of a
*request*, so every worker RPC must be safely retryable.

## 6. Retries and failure classification

Not all failures deserve the same treatment.

| Class | Meaning | Retryable by default |
| --- | --- | --- |
| `PROCESS_EXIT` | The workload ran and exited non-zero. | no |
| `TIMEOUT` | The workload exceeded its `timeout_seconds`. | no |
| `WORKER_LOST` | Lease expired or worker declared dead. | yes |
| `RESOURCE_ERROR` | Worker could not obtain resources / image pull failed. | yes |
| `SYSTEM_ERROR` | Executor or scheduler internal error. | yes |
| `CANCELED` | Operator or client canceled the job. | no (terminal) |

`PROCESS_EXIT` defaults to non-retryable because a deterministic program that exits 1
will exit 1 again; retrying it burns cluster capacity to produce the same answer. This
is configurable per job (`retry_on_process_exit`).

A retryable failure requeues the job if `attempt_count < max_attempts`, with
exponential backoff and full jitter:

```
delay = rand(0, min(base * 2^(attempt_count-1), max_delay))
```

The job becomes eligible for dispatch at `eligible_at = now + delay`. Jobs that are
queued but not yet eligible are counted separately in `atlas_jobs_queued{eligible=...}`
so that backoff is not mistaken for scheduling starvation.

When `attempt_count >= max_attempts`, the job moves to `FAILED` with the failure class
of the final attempt.

## 7. Invariants

These are checked mechanically by `internal/invariants` after every chaos run and in
integration tests. A violation is a test failure, not a warning.

**I1 — Terminal states are absorbing.** No job or attempt transitions out of
`SUCCEEDED`, `FAILED`, or `CANCELED`. Verified against the append-only `transitions`
audit table, not just the final row.

**I2 — Capacity is never oversubscribed.** For every worker,
`cpu_allocated <= cpu_capacity` and `memory_allocated <= memory_capacity`, and neither
allocation is ever negative.

**I3 — Allocation accounting is exact.** For every worker, the denormalized
`cpu_allocated` / `memory_allocated` equals the sum of the resource requests of its
non-terminal attempts. (The denormalized value exists for scheduling speed; this
invariant is what makes it trustworthy.)

**I4 — At most one live attempt per job.** A job has at most one attempt in a
non-terminal state, and if it has one, it is the job's `current_attempt_id`.

**I5 — Retries are bounded.** For every job, `attempt_count <= max_attempts`, and the
number of attempt rows equals `attempt_count`.

**I6 — Accepted jobs do not vanish.** Every `job_id` returned from a successful
`SubmitJob` exists in the store and, given a quiescent cluster with live workers,
eventually reaches a terminal state.

**I7 — Idempotency keys are unique.** No two jobs share an `idempotency_key`.

**I8 — No stale write took effect.** For every job, the attempt whose outcome is
reflected in the job's terminal state is the job's last attempt.

**I9 — Attempt and job states are consistent.** A job in `RUNNING` has a current
attempt in `RUNNING`; a `SUCCEEDED` job has a `SUCCEEDED` current attempt; and so on.

## 8. Consistency scope

Atlas runs **one scheduler process** at a time. There is no consensus protocol and no
replication, and this is a deliberate scope decision, not an oversight (see
`docs/ARCHITECTURE.md` §"What Atlas is not").

Concretely:

- All control-plane state lives in a single SQLite database on the scheduler's local
  disk, written under `journal_mode=WAL`, `synchronous=FULL`, with a single writer
  connection.
- Within that database, control-plane operations are serializable: each operation is one
  transaction, and the mutating path is serialized by a single connection.
- "Durable" means `fsync`'d by SQLite before the RPC is acknowledged. A job is
  acknowledged to the client only after its row is committed.
- Atlas is **not** highly available. If the scheduler process dies, the control plane is
  unavailable until it restarts. Running jobs keep running on workers; they are
  reconciled on restart. Atlas is crash-*recoverable*, not fault-*tolerant to scheduler
  loss* in the HA sense.
- Atlas does not survive loss of the scheduler's disk.

## 9. Scheduler restart

On startup the scheduler rebuilds its in-memory view purely from the store:

1. Load all workers; mark every worker `SUSPECT` (their heartbeat age is unknown and
   their connections are gone) and let heartbeats promote them back to `HEALTHY`.
2. Load all non-terminal jobs and their current attempts.
3. Recompute each worker's allocation from its live attempts (I3 is thereby
   re-established by construction).
4. Expire any lease whose `expires_at` already passed while the scheduler was down,
   declaring those attempts `LOST`.
5. Rebuild the ready queue from `QUEUED` jobs, preserving `enqueued_at` so priority
   aging is not reset by a restart.

Leases deliberately are **not** extended to account for scheduler downtime. A worker
that could not renew during the outage has lost its authority, which is the correct and
conservative reading: the scheduler could not observe it, so it must assume nothing.
The cost is some duplicate execution after a long scheduler outage, which is exactly
what at-least-once semantics permits, and which the chaos report measures.

## 10. Vocabulary we deliberately avoid

- We do not say **exactly-once**. We cannot defend it.
- We do not say **strongly consistent** without saying of what. Control-plane state in
  the single SQLite database is serializable. The cluster's view of *running work* is
  not, and cannot be: a worker's true state is only knowable up to the last successful
  RPC.
- We do not say **fault tolerant** unqualified. Atlas recovers from worker crashes,
  worker partitions, worker pauses, duplicate and reordered worker RPCs, and scheduler
  crashes. It does not survive scheduler disk loss or scheduler unavailability.

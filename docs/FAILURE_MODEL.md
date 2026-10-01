# Atlas Failure Model

"Fault tolerant" on its own means nothing. This document says which faults Atlas
handles, how, how fast, what it costs, and which faults it does not handle at all.

Every row in the first table has a corresponding test. Where a test exists it is
named, because a claimed behaviour with no test is a hope.

## 1. Faults Atlas handles

| Fault | Detected by | Detection bound | Response | Test |
| --- | --- | --- | --- | --- |
| Worker process killed (`SIGKILL`) | Missed heartbeats | `DeadAfter` (15s default) | All its attempts declared `LOST`, capacity released, jobs requeued | `TestWorkerDeathRecoversItsJobs` |
| Several workers killed at once | Missed heartbeats | `DeadAfter` | Same, per worker | `TestKillingMostOfTheFleetStillCompletesEveryJob` |
| Worker frozen (`SIGSTOP`, GC pause, hypervisor stall) | Missed heartbeats, then missed renewals | `DeadAfter`, or `LeaseTTL` if it keeps heartbeating | Attempts reclaimed; the worker is told to kill them on its next heartbeat | chaos `pause-worker` |
| Worker alive but stops renewing one lease | Lease expiry | `LeaseTTL` (15s default) | That attempt alone is reclaimed; the worker stays `HEALTHY` | `TestLeaseExpiryReclaimsAStuckWorker` |
| Worker restarts with the same id | Re-registration | Immediate | Its previous incarnation's attempts are reclaimed at once, without waiting out their leases | `TestWorkerRestartReclaimsItsOldWork` |
| Transient heartbeat loss | Missed heartbeats | `SuspectAfter` (6s default) | Worker goes `SUSPECT`: no new work, but it keeps what it has | `TestSuspectWorkerKeepsItsWork` |
| Scheduler killed (`SIGKILL`) | — | Restart time | State rebuilt from SQLite; lapsed leases reclaimed; queue rebuilt | `TestSchedulerRestartRecoversState` |
| Scheduler down longer than `LeaseTTL` | — | Restart time | Every lapsed lease reclaimed on startup, jobs requeued | `TestSchedulerRestartWithNoWorkersRequeuesEverything` |
| Corrupted allocation columns | Recovery | Restart time | Allocation recomputed from live attempts | `TestRecoveryRebuildsAllocationFromLiveAttempts` |
| A reclaimed worker reports a result later | Attempt scoping | Immediate | Rejected with `FAILED_PRECONDITION`; recorded as a duplicate execution | `TestStaleAttemptCannotOverwriteCurrentResult` |
| Forged or guessed lease id | Attempt scoping | Immediate | Rejected with `FAILED_PRECONDITION` | `TestForgedLeaseIsRejected` |
| Duplicated worker RPC (lost response) | Idempotent handlers | Immediate | Recorded outcome replayed; no second state change | `TestDuplicateCompletionIsIdempotent`, `TestDuplicateRPCsUnderLoad` |
| Duplicated client submission | Idempotency key | Immediate | Same `job_id` returned, `deduplicated=true` | `TestIdempotentSubmission` |
| Idempotency key reused with different parameters | Spec hash | Immediate | `ALREADY_EXISTS` rather than silently returning the old job | `TestIdempotencyKeyReuseWithDifferentSpecIsRejected` |
| Workload exits non-zero | Executor | Immediate | `PROCESS_EXIT`; not retried by default | `TestFailingJobIsNotRetriedByDefault` |
| Workload exceeds its timeout | Executor | `timeout_seconds` | Process group killed; `TIMEOUT`; not retried by default | `TestTimeoutKillsTheWorkload` |
| Job too large for any machine | Admission control | Immediate | `FAILED_PRECONDITION` at submit, rather than queueing forever | `TestAdmissionControlRejectsUnschedulableJobs` |
| Arrival rate beyond capacity | Admission control | Immediate | `RESOURCE_EXHAUSTED` once the queue is full | `TestAdmissionControlBoundsTheQueue` |
| Planned maintenance | Operator drain | Immediate | No new placements; running work finishes | `TestDrainStopsNewPlacementsButLetsRunningWorkFinish` |

## 2. The two failure detectors, and why there are two

Heartbeats and leases answer different questions, and conflating them produces a
system that is either too slow to react or too quick to duplicate work.

```
          ┌──────────────────────────────┐
          │  Is the worker process up?   │   heartbeats
          └──────────────┬───────────────┘
                         │
   HEALTHY ──missed for SuspectAfter──▶ SUSPECT ──missed for DeadAfter──▶ DEAD
      ▲                                    │                               │
      └────────── a heartbeat ─────────────┘                               │
                                                                           ▼
                                                      every attempt on it is reclaimed

          ┌──────────────────────────────────────────┐
          │  Does THIS worker still own THIS work?   │   leases
          └──────────────────┬───────────────────────┘
                             │
          attempt assigned ──┴──▶ renewed every LeaseTTL/4
                                     │
                                     └── not renewed for LeaseTTL ──▶ attempt LOST
```

Why both:

- A worker can be alive and heartbeating while making no progress on one job — its
  executor wedged, its disk full, one goroutine deadlocked. Heartbeats say nothing
  about that; the lease does.
- A worker can be partitioned in one direction, receiving heartbeat acknowledgements
  but unable to send results. The lease is what reclaims its work.
- Losing a worker is strong evidence about *all* of its attempts simultaneously.
  Waiting for each lease to expire individually would be slower for no benefit, so
  worker death is the fast path.

Why two heartbeat thresholds rather than one: a single missed heartbeat is common
and means almost nothing. `SUSPECT` stops the bleeding (no new work) without paying
the cost of reclamation. Collapsing the two would turn every GC pause into a round
of duplicate executions.

## 3. Timeline of a worker failure

With defaults (`LeaseTTL` 15s, `SuspectAfter` 6s, `DeadAfter` 15s, heartbeat 2s):

```
t=0.0s   worker W is running job J, attempt 1, lease L1
t=0.0s   W's machine loses power
t=6.0s   no heartbeat for 6s  → W marked SUSPECT, receives no new work
                                 (J is untouched; W might be coming back)
t=15.0s  no heartbeat for 15s → W marked DEAD
         ├─ attempt 1 → LOST
         ├─ W's allocation released
         ├─ J requeued with backoff, attempt_count now 1
         └─ dispatcher woken
t≈15.1s  J assigned to worker X as attempt 2, lease L2
```

The worst case for a *single* stuck attempt on an otherwise healthy worker is
`LeaseTTL`, since heartbeats keep flowing and only the renewal stops.

These defaults are tuned for a single datacentre, where a 15-second silence really
does mean something is wrong. A deployment across higher-latency links should raise
all three, keeping the ratios: `SuspectAfter ≈ 3 heartbeats`, `DeadAfter ≈ LeaseTTL`.

## 4. The cost of recovery

Atlas is at-least-once. Recovery is not free, and the price is paid in duplicate
work:

- A reclaimed attempt may have been succeeding. Atlas cannot tell, so it retries,
  and the workload runs twice. `atlas_duplicate_executions_detected_total` counts
  the cases Atlas observes directly — a reclaimed worker reporting success after the
  fact — but it is a lower bound, not the true count. A worker that dies without
  ever reporting leaves no trace of whether its work completed.
- Backoff is applied before a retry, so a mass failure does not produce a
  thundering herd. Full jitter means the retried jobs are spread over the backoff
  window rather than arriving together.
- A worker declared `DEAD` that comes back re-registers, which reclaims anything it
  still believes it holds.

The mitigation available to a workload is the one Atlas provides the tools for:
`ATLAS_JOB_ID` is stable across retries and makes a natural idempotency key for
whatever the workload writes.

## 5. Faults Atlas does not handle

| Fault | What happens | Why not |
| --- | --- | --- |
| Scheduler machine lost | Control plane unavailable until it is restarted somewhere with the database | No replication; single scheduler by design (`docs/ARCHITECTURE.md`) |
| Scheduler disk lost | Permanent loss of all control-plane state | No replication. Back up the SQLite file if you care. |
| Two schedulers on one database | Undefined. Do not do it. | No leader election. The write path assumes a single writer. |
| Byzantine worker | A worker that lies about results is believed | Atlas authenticates nothing; it assumes a trusted network |
| Network eavesdropping or tampering | No protection | gRPC runs without TLS |
| Clock skew between scheduler and workers | Harmless | Every lease deadline is computed and compared on the scheduler. Workers never evaluate expiry; they only renew. |
| Workload corrupts shared external state | Atlas cannot help | At-least-once means the workload must be idempotent |
| Disk full on the scheduler | Transactions fail, submissions error, `/ready` fails | Detected and surfaced, not survived |

The clock row is worth dwelling on: Atlas deliberately never compares timestamps
across machines. `lease_expires_at` is written by the scheduler and compared against
the scheduler's own clock. A worker with a wildly wrong clock still renews on the
right schedule, because it works in intervals rather than deadlines.

## 6. What a chaos campaign actually exercises

`make chaos-campaign` runs real processes and injects, at randomized intervals:

- `SIGKILL` on a worker, with a restart shortly after
- `SIGSTOP` on a worker, held for 1–4 seconds, then `SIGCONT` — the cruellest fault,
  because the worker is alive, holds its leases, and answers nothing
- `SIGKILL` on the scheduler, with a restart after 200–1000ms
- a configurable fraction of heartbeats dropped
- a configurable fraction of lease renewals dropped
- a configurable fraction of completion reports replayed

It never kills the last remaining worker, because a cluster with no capacity cannot
make progress and the run would be measuring a timeout rather than recovery. That is
a limitation of the harness, stated here rather than hidden.

Afterwards it reads the database and checks all nine invariants from
`docs/SEMANTICS.md` §7, and exits non-zero on any violation.

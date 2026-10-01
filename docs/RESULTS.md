# Atlas Results

Every number here was measured on the machine described below. Methodology, metric
definitions, and the limitations that apply to each tool are in
[`docs/BENCHMARKING.md`](BENCHMARKING.md); read that before quoting anything from
this file. Raw output is in `results/`, reproducible with `make experiments` and
`make chaos-campaign`.

```
host         cursor (container)
go           go1.22.2
os/arch      linux/amd64
cpus         8
memory       47.1 GiB
sqlite       modernc.org/sqlite v1.29.10 (pure Go), WAL, synchronous=FULL
```

One machine, one run per configuration, no repeated trials. The differences called
out below are factors rather than percentages, so run-to-run noise does not explain
them; the small differences inside the tables should not be read as real.

---

## Experiment 1 — Placement policy versus offered load

100 heterogeneous workers (50 × 16 cpu/32 GiB, 30 × 32 cpu/16 GiB, 20 × 8 cpu/128
GiB), the standard mixed workload, Poisson arrivals, 20,000 jobs per run, FIFO
ordering, seed 1. Estimated sustainable throughput is 22.3 jobs/sec; load is
expressed as a fraction of that.

| Load | Policy | Mean wait | p95 | p99 | CPU util | Stranded CPU | Stranded mem | Peak queue |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 70% | round-robin | 1.33s | 386ms | 49.0s | 41.4% | 34.9% | 42.4% | 53 |
| 70% | **least-loaded** | **128ms** | **0ms** | **3.82s** | 41.4% | **29.7%** | **37.4%** | **24** |
| 70% | best-fit | 2.92s | 22.1s | 47.8s | 41.1% | 33.7% | 41.3% | 212 |
| 90% | round-robin | 9.80s | 6.97s | 5.4m | 46.6% | 35.3% | 42.7% | 347 |
| 90% | **least-loaded** | **2.18s** | **0ms** | **93.0s** | 46.9% | **22.1%** | **31.0%** | **119** |
| 90% | best-fit | 11.60s | 82.2s | 4.4m | 46.9% | 32.0% | 40.1% | 568 |
| 105% | round-robin | 15.16s | 33.2s | 7.5m | 46.5% | 35.5% | 42.8% | 588 |
| 105% | **least-loaded** | **5.36s** | **18.8s** | **3.1m** | **50.3%** | **21.1%** | **30.2%** | **311** |
| 105% | best-fit | 14.51s | 69.5s | 6.0m | 48.9% | 30.5% | 38.6% | 668 |

Least-loaded wins at every load, by a factor of 3.5× on p99 wait at 90% and by
roughly a third on stranded capacity. Best-fit — the policy that sounds like it
should pack better — is the worst of the three on p95 wait at every load.

The p50 column is 0ms everywhere and is omitted: most jobs in this mix are small
and place immediately. The whole difference between policies lives in the tail,
which is the usual shape for a scheduler and the reason p99 is the number to read.

---

## Experiment 1b — Resource fragmentation

The clearest result in the project, and the most counterintuitive.

Two fleets with **identical total capacity** — 384 cores, 928 GiB, 20 machines —
differing only in the shape of the machines. Same workload, same arrival sequence,
same seed, 90% offered load.

```
uniform:       20 × (19.2 cpu, 46.4 GiB)
heterogeneous: 10 × (16 cpu, 32 GiB) + 6 × (32 cpu, 16 GiB) + 4 × (8 cpu, 128 GiB)
```

| Fleet | Policy | Mean wait | p99 wait | CPU util | Mem util | Stranded CPU | Stranded mem |
| --- | --- | --- | --- | --- | --- | --- | --- |
| uniform | round-robin | 2.61s | 55.9s | 81.1% | 71.6% | 8.9% | 19.6% |
| uniform | least-loaded | 2.65s | 61.0s | 81.1% | 71.6% | 10.3% | 20.6% |
| uniform | best-fit | **2.48s** | **51.7s** | 81.0% | 71.5% | **8.3%** | **19.1%** |
| heterogeneous | round-robin | 24.80s | 12.4m | 72.5% | 64.0% | 22.8% | 31.8% |
| heterogeneous | least-loaded | **12.08s** | **6.7m** | **76.1%** | **67.1%** | **18.1%** | **27.7%** |
| heterogeneous | best-fit | **89.24s** | **19.6m** | 68.0% | 60.0% | 27.9% | 36.4% |

### Two things to take from this

**Heterogeneity costs more than policy choice.** Moving the same workload from a
uniform fleet to a heterogeneous one of identical total capacity multiplies mean
wait by 5× under the best policy and by 36× under the worst. The resources are all
still there; they just stop being reachable by the jobs that need them. CPU
utilization falls from 81% to 68–76% with no change in demand.

**Best-fit inverts.** It is marginally the best policy on the uniform fleet (2.48s)
and by far the worst on the heterogeneous one (89.2s, 7.4× worse than
least-loaded). The mechanism is specific: best-fit minimizes leftover capacity, so
it preferentially spends *scarce, specifically-shaped* resources on jobs that did
not need them. A 1-core / 12 GiB memory-heavy job "best-fits" onto a 32-core /
16 GiB compute node, because 16 GiB leaves less slack than the 128 GiB memory
node's would. That single placement consumes most of the compute node's memory and
blocks the CPU-heavy work the node exists to run.

The lesson generalizes: tight packing is only a virtue when the thing you are
packing into is interchangeable. On a heterogeneous fleet, the leftover a greedy
policy is minimizing is the wrong quantity — what matters is preserving the
capacity shapes that are rare.

---

## Experiment 2 — Worker failure

### 2a. Real processes (integration tests)

| Scenario | Workers | Jobs | Result |
| --- | --- | --- | --- |
| Kill 1 of 3 workers mid-flight | 3 | 30 | 30/30 succeeded, 2 attempts LOST, 2 retries |
| Kill 2 of 6 workers mid-flight | 6 | 60 | 60/60 succeeded, 2 attempts LOST, max 2 attempts on any job |

Logical jobs lost in both cases: **0**. Capacity held by the dead workers was fully
released; the cluster's allocated CPU returned to zero once the work drained.

### 2b. Failure rate versus turnaround (simulator)

50 workers, mixed workload, 20 jobs/sec, 5-attempt budget, 15s lease TTL, 30s
worker recovery.

| Failures/worker/hour | Failures | Lost attempts | Retries | Completed | Abandoned | Mean turnaround | p99 turnaround |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 0 | 0 | 20,000 | 0 | 3.5m | 23.5m |
| 1 | 1,199 | 134 | 134 | 20,000 | 0 | 4.1m | 23.3m |
| 10 | 10,949 | 1,471 | 1,463 | 19,992 | 8 | 5.9m | 35.3m |
| 60 | 48,067 | 9,605 | 8,749 | 19,144 | 856 | 7.0m | 52.2m |

Degradation is graceful rather than cliff-edged. At one failure per worker per hour
— already a terrible fleet — nothing is lost at all and mean turnaround rises 17%.
At one failure per worker per *minute*, 4.3% of jobs exhaust a 5-attempt budget.
The budget, not the failure rate, is what decides whether a job eventually fails,
which is the intended design: `max_attempts` is the knob.

Note that lost attempts are far fewer than failures (134 from 1,199) because most
failures hit a worker with nothing running on it.

---

## Experiment 3 — Scheduler crash recovery

Real processes, real SQLite, `SIGKILL` with no graceful shutdown.

| Scenario | Result |
| --- | --- |
| Crash with 10 finished and 20 in-flight jobs, workers surviving | All 30 jobs terminal afterwards; the 10 already-succeeded jobs unchanged; 0 lost; 0 invariant violations |
| Crash with the fleet gone and downtime > lease TTL | All 8 in-flight jobs requeued by recovery; allocated CPU back to 0; all ran when a worker returned |
| Crash with the allocation columns deliberately corrupted while down | Allocation recomputed exactly from live attempts on startup; invariants clean |

Leases are deliberately **not** extended to account for scheduler downtime. A worker
that could not renew during the outage has lost its authority. That is the
conservative reading — the scheduler could not observe it, so it assumes nothing —
and the cost is some duplicate execution after a long outage, which is exactly what
at-least-once permits and what the chaos campaign below measures.

---

## Experiment 4 — Overload and admission control

20 uniform workers (16 cpu / 32 GiB), uniform 2-core / 4 GiB jobs with
exponential 10s runtimes. Sustainable throughput is about 16 jobs/sec.

| Arrival/s | Admission | Accepted | Rejected | Peak queue | Mean wait | p99 wait | CPU util |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | none | 20,000 | 0 | 1 | 0ms | 0ms | 50.3% |
| 8 | queue ≤ 500 | 20,000 | 0 | 1 | 0ms | 0ms | 50.3% |
| 16 | none | 20,000 | 0 | 548 | 20.6s | 33.5s | 96.5% |
| 16 | queue ≤ 500 | 19,961 | 39 | 500 | 19.7s | 31.3s | 96.5% |
| 32 | none | 20,000 | 0 | **10,178** | **5.3m** | **10.5m** | 97.1% |
| 32 | queue ≤ 500 | 10,296 | 9,704 | 500 | 29.6s | **34.7s** | 95.0% |
| 64 | none | 20,000 | 0 | **14,946** | 7.8m | **15.6m** | 97.2% |
| 64 | queue ≤ 500 | 5,525 | 14,475 | 500 | 28.6s | **33.7s** | 90.0% |
| 128 | none | 20,000 | 0 | **17,451** | 9.1m | **18.1m** | 97.3% |
| 128 | queue ≤ 500 | 3,109 | 16,891 | 500 | 26.7s | **33.3s** | 81.0% |

At and below capacity the two configurations are indistinguishable, which is the
point: admission control costs nothing until it is needed.

Above capacity they diverge completely. Without it, **every submission succeeds** —
the caller gets a job id and a `QUEUED` state and no indication whatsoever that
anything is wrong — while p99 wait climbs to 18 minutes and the queue reaches
17,451. With a bounded queue, p99 wait stays flat at ~33 seconds across an 8×
range of offered load, and the excess arrives back at the caller immediately as
`RESOURCE_EXHAUSTED`.

The honest cost is visible too: at 128/sec the bounded queue's CPU utilization
drops to 81%, because a queue of 500 is sometimes too shallow to keep every worker
fed through a dip. Backpressure trades a little utilization for a bounded,
observable latency. That is usually the right trade, and it is a trade rather than
a free win.

---

## Experiment 5 — Placement decision cost

`Policy.Select` called directly, 200,000 decisions per row, no queue and no event
loop. Three scenarios: an idle fleet, a busy (80% allocated) fleet, and a busy fleet
with a job no worker can hold — the dispatcher's worst case, and a realistic one.

| Workers | Scenario | round-robin | least-loaded | best-fit |
| --- | --- | --- | --- | --- |
| 10 | idle, small job | 35ns | 54ns | 52ns |
| 10 | busy, fits nowhere | 55ns | 36ns | 37ns |
| 100 | idle, small job | 36ns | 238ns | 245ns |
| 100 | busy, fits nowhere | 285ns | 126ns | 115ns |
| 1,000 | idle, small job | 35ns | 2.09µs | 2.17µs |
| 1,000 | busy, fits nowhere | 2.57µs | 1.03µs | 844ns |
| 10,000 | idle, small job | 35ns | 21.1µs | 21.5µs |
| 10,000 | busy, small job | 35ns | 20.9µs | 21.9µs |
| 10,000 | busy, fits nowhere | 25.3µs | 9.2µs | 8.1µs |

**The O(workers) scan is not a problem at these sizes.** At 10,000 workers,
least-loaded costs 21 µs per decision — about 47,000 placements per second on a
single core, which is far more than a cluster of that size generates. There is no
case for indexing workers by available capacity until the fleet is an order of
magnitude larger, and the simpler linear scan is worth keeping until then.

**Round-robin's cost is bimodal, not low.** It is flat at 35ns whenever some worker
fits, because it stops at the first one — and 25 µs when none does, making it the
most expensive policy in the table precisely when the cluster is under pressure.
A policy whose cost depends on cluster state is harder to reason about than one
that is uniformly linear.

**Failed placements are cheaper than successful ones** for the scoring policies
(9.2 µs versus 20.9 µs at 10,000 workers), because a worker that fails the fit
check is never scored. The expensive case is success.

### Queue operations (`go test -bench`)

| Depth | fifo | priority-aging | edf |
| --- | --- | --- | --- |
| 100 | 159ns | 192ns | 176ns |
| 10,000 | 172ns | 202ns | 348ns |
| 100,000 | 166ns | 213ns | 442ns |

This is the design claim of the queue, measured. The bucketed orderings are flat
from depth 100 to depth 100,000, because the best job in a priority bucket is
always its oldest and the global best is found by comparing bucket heads — O(number
of distinct priorities), not O(queued jobs). EDF uses a real heap, because deadline
order is not monotone in arrival order, and shows the expected O(log n) growth.

---

## Experiment 6 — Priority aging versus starvation

10 workers, deliberately undersized, 40 jobs/sec. 70% of jobs at priority 100
("urgent"), 30% at priority 0 ("batch"). Split by class, because the aggregate hides
the entire phenomenon.

| Ordering | urgent mean | urgent p99 | batch mean | batch p99 | batch max |
| --- | --- | --- | --- | --- | --- |
| fifo (priority ignored) | 6.4m | 12.6m | 6.4m | 12.6m | 12.7m |
| strict priority | **3.2m** | 6.3m | **13.6m** | 14.5m | 14.5m |
| aging 1/s, cap 1000 | 5.2m | 11.4m | 9.1m | 13.1m | 13.2m |
| aging 10/s, cap 1000 | 3.8m | 7.0m | 12.3m | 14.4m | 14.4m |
| aging 10/s, **no cap** | 6.3m | 12.5m | 6.7m | 12.8m | 12.8m |

CPU utilization is 98.6–98.8% in every row: the ordering decides *who* waits, not
how much work gets done.

Strict priority halves the urgent class's wait (6.4m → 3.2m) and charges the batch
class 2.1× for it (6.4m → 13.6m). That is the trade working as intended.

### The cap is the trap

Aging at 10/s **with a cap of 1000** behaves almost exactly like strict priority
(batch 12.3m versus 13.6m), while the *same rate uncapped* collapses to FIFO (batch
6.7m, urgent 6.3m).

The mechanism: aging adds `rate × wait` to both classes equally, so a batch job
overtakes an urgent one only once it has waited `100 / rate` seconds longer. At
10/s that is 10 seconds, which under this load is nothing — aging should dominate
completely. But once *every* queued job has waited long enough to saturate the cap,
both classes sit at `base + 1000`, and the only thing separating them is their base
priority again. The cap silently restores the starvation it was added to prevent.

So the cap is not a safety rail on aging; it is the dial that decides how much
aging you actually get. Setting it to a value every job reaches disables the
feature. The useful configuration is the 1/s row: enough aging to bound the batch
class's wait at 9.1m rather than 13.6m, while still giving urgent work a real
advantage (5.2m versus 6.4m under FIFO).

---

## The chaos campaign

The headline run. Real `atlas-server`, real `atlas-worker` processes, real SQLite,
real `SIGKILL`.

```
workers                   10
jobs                      3,000
job duration              1.5s
attempt budget            10
fault window              2m30s (mean interval 2s)
lease ttl                 4s
heartbeat suspect / dead  1.5s / 4s
duplicate rpc rate        0.20
heartbeat drop rate       0.10
lease renewal drop rate   0.05
seed                      1
wall clock                2m46s
```

**Faults injected: 59**

| Fault | Count |
| --- | --- |
| `SIGKILL` a worker (then restart it) | 31 |
| `SIGSTOP` a worker for 1–4s, then `SIGCONT` | 23 |
| `SIGKILL` the scheduler (then restart it) | 5 |

Plus 20% of completion reports replayed, 10% of heartbeats dropped, and 5% of lease
renewals dropped, throughout.

**Outcome**

```
submitted                 3,000
accepted                  3,000
rejected (admission)      0
failed to submit          0

jobs in store             3,000
terminal                  3,000
succeeded                 3,000
failed after retry limit  0
still pending             0

attempts total            3,105
  succeeded               3,000
  lost (lease reclaimed)  105
retries                   105
most attempts on one job  3
duplicate executions      11

recovery latency   p50 66ms   p99 143ms   max 198ms
                   (excluding detection, bounded by the 4s dead-after
                    threshold or the 4s lease TTL)

invariants checked against 24,626 audited state changes
  I1  terminal states were never left                      held
  I2  no worker was oversubscribed or went negative        held
  I3  allocation equals the sum of live attempts           held
  I4  at most one live attempt per job                     held
  I5  attempt counts stayed within budget                  held
  I6  every accepted job is still in the store             held
  I7  idempotency keys are unique                          held
  I8  no stale attempt decided a job's outcome             held
  I9  job and attempt states agree                         held

VERDICT: PASS
```

### Reading this honestly

**Logical jobs lost: 0.** Every one of the 3,000 jobs that was acknowledged to a
client reached a terminal state, and every one of them succeeded, through 54 worker
failures and 5 scheduler restarts.

**Duplicate executions: 11.** This is the number that matters most, and it is not
zero. Eleven times, a worker had its lease reclaimed, the job was given to someone
else, and the original worker then reported that it had in fact finished. Those
jobs really did run twice. Atlas rejected the late reports — invariant I8 held, so
none of them changed a job's outcome — but the work happened twice, and that is
what at-least-once execution means. A chaos report claiming zero here would either
be measuring nothing or lying.

It is also a *lower bound*. Atlas can only count the duplicates it observes: a
worker that dies mid-job without ever reporting leaves no evidence of whether its
work completed, and nothing can recover that evidence afterwards.

**Recovery is fast once detected, and detection is the slow part.** The p99 from
reclamation to reassignment is 143ms. The 4-second dead-after threshold dominates
the end-to-end recovery time by a factor of about 30, which is the right place for
the cost to be: it is the one parameter an operator tunes against their network.

**105 retries from 54 worker faults.** Roughly two in-flight attempts per killed or
paused worker, with the fleet running 10 workers at 1.5-second jobs. No job needed
more than 3 of its 10 attempts.

Reproduce with:

```bash
make build
./bin/atlas-chaos --workers 10 --jobs 3000 --duration 150s --job-duration 1500ms \
  --fault-interval 2s --drain-timeout 240s --restart-scheduler \
  --duplicate-rpc-rate 0.2 --heartbeat-drop-rate 0.1 --renew-drop-rate 0.05 \
  --max-attempts 10 --seed 1 --report results/chaos-campaign.txt
```

---

## Test suite

| Suite | Count | What it covers |
| --- | --- | --- |
| `internal/state` | 6 | The transition table exhaustively, including that every non-terminal state can reach a terminal one |
| `internal/store` | 11 | Transactionality, rollback, capacity enforcement, the audit log, idempotency uniqueness |
| `internal/scheduler` | 23 | Queue orderings, aging, backfill ordering, fit, each policy, backoff bounds and jitter |
| `internal/invariants` | 11 | Each invariant, against a database built to violate exactly that one |
| `internal/worker` | 12 | Output capture, exit-code classification, timeouts, process-group kill, and the identifiers a workload needs to be idempotent |
| `internal/types` | 5 | Resource arithmetic and the size/CPU parsers |
| `simulator/` | 11 | Determinism for a seed, job conservation, no oversubscription inside the model, and that the failure path is actually wired up |
| `tests/` (integration) | 26 | Real gRPC, real SQLite, real worker processes: lifecycle, idempotency, cancellation, admission, draining, worker death, scheduler restart, stale attempts, forged leases, duplicate RPCs |

All pass under `-race`. The integration suite takes about 33 seconds; with the race
detector, about 42.

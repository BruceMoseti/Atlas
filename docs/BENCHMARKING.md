# Benchmarking Methodology

Measured numbers live in [`docs/RESULTS.md`](RESULTS.md). This document says how
they were produced and, more importantly, what each one does and does not mean.

## The three measurement tools, and what each can actually claim

Atlas measures itself three different ways, because the three questions it needs to
answer are not answerable by the same instrument.

| Tool | What it runs | What it can claim | What it cannot |
| --- | --- | --- | --- |
| `go test -bench` (`internal/scheduler`) | One function, repeatedly | The cost of a single placement decision or queue operation | Anything about system behaviour |
| `atlas-sim` | The real queue, policies, and resource model against a virtual clock | How a scheduling policy behaves on a given workload and fleet, at sizes no laptop could run | Anything about the network, the database, durability, or real failure timing |
| `atlas-chaos` | Real processes, real SQLite, real `SIGKILL` | That the documented invariants held under real partial failure, and how fast recovery was | Throughput at scale, because it is bounded by one machine |

The division is deliberate. A simulator that claimed to measure durability would be
lying; a chaos campaign that claimed to measure 10,000-worker scheduling behaviour
would be lying differently.

### What the simulator replaces

`atlas-sim` imports `internal/scheduler` and uses `ReadyQueue`, `Policy`,
`WorkerView`, and `Resources` — the same code `atlas-server` runs. What it replaces
is everything below the decision:

- **No network.** Assignment is a function call.
- **No database.** No transactions, no fsync, no recovery.
- **Virtual clock.** A simulated hour costs whatever the event loop costs.
- **Modelled execution.** A job "runs" by scheduling a completion event at
  `now + runtime`; nothing is executed.
- **Modelled failure.** A worker "fails" by having its jobs requeued one `LeaseTTL`
  later. Real detection involves heartbeats, timers, and transactions.

So a simulator result of the form "least-loaded reduced p99 queue wait by 2×" is a
claim about *the policy*, holding the workload fixed. It is not a claim about
`atlas-server`'s throughput.

## Environment

Every report begins with a machine block, emitted by `simulator.Environment()`:

```
  run at       2026-10-01T03:22:08Z
  host         cursor
  go           go1.22.2
  os/arch      linux/amd64
  cpus         8
  memory       47.1GiB
```

A throughput number without the hardware it was measured on is not a result. The
numbers in `docs/RESULTS.md` were produced on that machine; reproduce them on yours
before comparing.

Software versions: Go 1.22.2, SQLite via `modernc.org/sqlite` v1.29.10 (pure Go, no
cgo), gRPC v1.64.1, `journal_mode=WAL`, `synchronous=FULL`.

## Reproducing

```bash
make build

# All six simulator experiments, written to ./results
make experiments

# Individually
./bin/atlas-sim policy        --workers 100 --jobs 20000 --seed 1
./bin/atlas-sim fragmentation --jobs 20000 --seed 1
./bin/atlas-sim priority      --jobs 20000 --seed 1
./bin/atlas-sim failure       --jobs 20000 --seed 1
./bin/atlas-sim overload      --jobs 20000 --seed 1
./bin/atlas-sim scale         --decisions 200000 --seed 1

# Microbenchmarks
make bench

# Chaos campaign (exits non-zero on any invariant violation)
make chaos-campaign
```

Every simulator run is deterministic for a given `--seed`: arrivals, job classes,
runtimes, and failure times are all drawn from one seeded generator before the event
loop starts, and the event queue breaks ties on a monotonic sequence number. Two
runs with the same seed and configuration produce identical output.

## Experimental design

### Controls

Each experiment varies exactly one thing and holds everything else fixed, including
the seed. In the policy comparison, the fleet, the workload, the arrival sequence,
the queue ordering, and the random draws are identical across policies — the only
difference is which worker gets chosen. Any difference in the results is therefore
attributable to that choice.

The fragmentation experiment goes further and holds *total capacity* equal between
the uniform and heterogeneous fleets: same core count, same bytes, same number of
machines. Only the shape differs.

### Offered load, not job count

Policy quality is only visible in a particular regime. On an idle cluster every
policy puts the job somewhere and it runs immediately. Under a permanently saturated
queue the backfill window finds *something* that fits no matter how bad the last
decision was. Both regimes make every policy look identical.

So the experiments run at a controlled fraction of capacity. `SustainableRate`
computes the arrival rate a fleet could retain under perfect packing:

```
rate = min( fleet_cpu / E[cpu × runtime],  fleet_memory / E[memory × runtime] )
```

That is an upper bound — perfect packing is not achievable — which makes it the
right denominator. A run at 90% of it is genuinely near capacity, and any queueing
that appears there is the packing losing ground rather than the cluster simply being
too small.

### Workload

The default mix (`MixedWorkload`) is four classes:

| Class | Share | CPU | Memory | Runtime |
| --- | --- | --- | --- | --- |
| small | 60% | 0.5 | 512 MiB | bimodal, mean 5s with a 5% tail at 60s |
| cpu-heavy | 20% | 4 | 1 GiB | bimodal, mean 20s with a 10% tail at 180s |
| memory-heavy | 15% | 1 | 12 GiB | bimodal, mean 30s with a 10% tail at 200s |
| large | 5% | 8 | 16 GiB | exponential, mean 120s |

Two properties of this mix are load-bearing. The resource shapes differ along *both*
dimensions, so a policy that optimizes one can strand the other — that is what makes
fragmentation measurable at all. And the runtimes are heavy-tailed, so a badly
placed long job holds its mistake for a long time. A workload of identical jobs
would show nothing.

## Metric definitions

Precise definitions, because most of these have several plausible meanings.

**Wait** — virtual seconds from a job arriving to its *first* assignment. Excludes
retries.

**Turnaround** — arrival to final completion, including every retry and every
backoff. This is what the submitter experiences.

**Percentiles** — nearest-rank on the sorted sample, no interpolation.

**CPU / memory utilization** — time-weighted mean of `allocated / schedulable
capacity`, averaged over the run's *active* time (any allocation or any queued job).
Idle time at the tail of a run is excluded, because including it would drag every
policy toward zero and hide the differences.

**Stranded CPU / memory** — time-weighted mean of `free / capacity`, averaged over
exactly the time at least one job was *waiting for capacity*. This is fragmentation
made measurable: resources that existed, were idle, and still could not be used.
Note the denominator differs from utilization's, so the two do not sum to 100%.

**Peak queue depth** — the largest number of jobs waiting simultaneously.

**Decision cost** (`scale`) — wall-clock time inside `Policy.Select` only. No queue,
no event loop, no allocation of the fleet. Occupancy is set explicitly because how
far a policy has to look before finding a fit is the thing being measured.

**Recovery latency** (chaos) — from an attempt being reclaimed to its replacement
being assigned. It deliberately *excludes* detection time, which is bounded above by
`DeadAfter` for a worker that stops heartbeating and by `LeaseTTL` for one that is
alive but stops renewing. Reporting them together would hide which one you can tune.

## Known limitations

These are the reasons to distrust specific numbers, listed so a reader does not have
to find them.

1. **Simulator placements are instantaneous.** Real assignment costs a gRPC round
   trip and a shared `fsync`. The simulator's wait times are therefore optimistic by
   roughly one dispatch interval plus one commit.
2. **The simulator's failure model is crude.** A failed worker's jobs reappear
   exactly `LeaseTTL` later. Reality involves two heartbeat thresholds, a reconcile
   tick, and a transaction. Use the chaos campaign for recovery timing; use the
   simulator only for the shape of the relationship between failure rate and
   turnaround.
3. **Chaos runs are single-machine.** Eight worker processes on eight cores contend
   for the same CPU, so absolute timings are pessimistic and the fault injection is
   coarser than a real fleet's.
4. **The chaos harness never kills the last worker.** A cluster with no capacity
   cannot make progress and the run would be measuring a timeout.
5. **Duplicate execution counts are a lower bound.** Atlas counts the cases it
   observes directly — a reclaimed worker reporting success afterwards. A worker
   that dies mid-job without ever reporting leaves no evidence of whether its work
   completed, and no system can recover that evidence after the fact.
6. **`SustainableRate` assumes perfect packing** and treats CPU and memory
   independently. It is a bound, not a prediction.
7. **No Docker measurements.** The Docker executor is implemented and documented but
   was never exercised: the development environment had no Docker daemon. Only the
   process executor has been run. This is stated plainly rather than quietly
   omitted.
8. **One machine, one run per configuration.** No repeated trials, no confidence
   intervals. The differences reported in `docs/RESULTS.md` are large enough
   (factors, not percentages) that run-to-run noise does not explain them, but the
   small differences in those tables should not be read as real.

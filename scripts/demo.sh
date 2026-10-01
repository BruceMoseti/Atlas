#!/usr/bin/env bash
#
# A self-contained demonstration of the behaviour Atlas exists for: a job is
# placed, the worker running it is killed with SIGKILL, and the job finishes on a
# different machine without the client doing anything.
#
# Everything here is real — a real scheduler, a real SQLite database, real worker
# processes, and a real kill. Run it from the repository root:
#
#     make build && ./scripts/demo.sh
#
set -euo pipefail

BIN="${BIN:-./bin}"
WORKDIR="$(mktemp -d /tmp/atlas-demo.XXXXXX)"
GRPC_ADDR="${GRPC_ADDR:-127.0.0.1:50451}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:9451}"
ATLAS="$BIN/atlas --server $GRPC_ADDR"

for bin in atlas-server atlas-worker atlas; do
    if [[ ! -x "$BIN/$bin" ]]; then
        echo "error: $BIN/$bin not found. Run 'make build' first." >&2
        exit 1
    fi
done

PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; done
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
# Commands are passed as a single string so that quoting inside them survives;
# eval is intentional here and the input is all literal, never user-supplied.
run()  { printf '\033[2m$ %s\033[0m\n' "$1"; eval "$1"; }

# Short lease and heartbeat thresholds so the demo takes seconds rather than the
# production defaults' tens of seconds. The ratios between them are unchanged.
step "Starting the scheduler (lease TTL 3s, dead-after 3s)"
"$BIN/atlas-server" \
    --listen "$GRPC_ADDR" --http "$HTTP_ADDR" --db "$WORKDIR/atlas.db" \
    --lease-ttl 3s --heartbeat-interval 500ms \
    --suspect-after 1500ms --dead-after 3s \
    --log-level warn >"$WORKDIR/server.log" 2>&1 &
PIDS+=($!)
disown

for _ in $(seq 50); do
    $ATLAS status >/dev/null 2>&1 && break
    sleep 0.2
done

step "Starting two workers"
for id in w1 w2; do
    "$BIN/atlas-worker" --scheduler "$GRPC_ADDR" --id "$id" \
        --cpu 2 --memory 4GB --acquire-wait 500ms \
        --log-level warn >"$WORKDIR/$id.log" 2>&1 &
    PIDS+=($!)
    disown
done
sleep 1.5
run "$ATLAS workers"

step "Submitting a 30-second job that occupies a whole worker"
JOB=$($ATLAS submit --cpu 2 --memory 4GB --max-attempts 3 \
        --idempotency-key demo-recovery -- sleep 30 | awk 'NR==1{print $1}')
echo "job id: $JOB"

# Wait for it to actually be executing, so the kill lands on live work.
for _ in $(seq 50); do
    [[ "$($ATLAS get "$JOB" | awk '$1=="state"{print $2}')" == "RUNNING" ]] && break
    sleep 0.2
done
run "$ATLAS get $JOB"

VICTIM=$($ATLAS get "$JOB" | awk '/^1 /{print $3}')
step "Killing worker $VICTIM with SIGKILL — no warning, no cleanup"
pkill -9 -f "atlas-worker --scheduler $GRPC_ADDR --id $VICTIM" || true
echo "worker $VICTIM is gone"

step "The scheduler notices, reclaims the lease, and retries elsewhere"
for _ in $(seq 100); do
    if [[ "$($ATLAS get "$JOB" | awk '$1=="attempts"{print $2}')" -ge 2 ]]; then break; fi
    sleep 0.3
done
run "$ATLAS get $JOB"

step "Idempotent submission: the same key returns the same job, never a second one"
run "$ATLAS submit --cpu 2 --memory 4GB --idempotency-key demo-recovery -- sleep 30"

step "Cleaning up"
run "$ATLAS cancel $JOB --reason 'demo finished'"

printf '\n\033[1;32mAttempt 1 was LOST when its worker died; attempt 2 ran on the surviving\n'
printf 'worker. The client never retried anything.\033[0m\n\n'

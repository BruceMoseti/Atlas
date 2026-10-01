GO            ?= go
GOTOOLCHAIN   ?= local
BIN           := bin
PROTOC        ?= protoc
PKG           := github.com/BruceMoseti/Atlas

export GOTOOLCHAIN

.PHONY: all build test test-race test-integration lint fmt vet proto clean \
        chaos chaos-campaign bench bench-policy bench-scale bench-overload \
        experiments tools

all: build

## build: compile every binary into ./bin
build:
	$(GO) build -o $(BIN)/atlas-server ./cmd/atlas-server
	$(GO) build -o $(BIN)/atlas-worker ./cmd/atlas-worker
	$(GO) build -o $(BIN)/atlas         ./cmd/atlas-cli
	$(GO) build -o $(BIN)/atlas-chaos   ./cmd/atlas-chaos
	$(GO) build -o $(BIN)/atlas-sim     ./cmd/atlas-sim

## test: unit tests
test:
	$(GO) test ./internal/... ./simulator/...

## test-race: unit tests under the race detector
test-race:
	$(GO) test -race ./internal/... ./simulator/...

## test-integration: end-to-end tests, including worker and scheduler kills
test-integration: build
	$(GO) test -timeout 15m -tags integration ./tests/...

## test-all: everything, which is what CI runs
test-all: test-race test-integration

fmt:
	gofmt -w ./cmd ./internal ./simulator ./chaos ./tests ./benchmarks

vet:
	$(GO) vet ./...

lint: vet
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed; skipping"

## proto: regenerate Go code from proto/atlas.proto
proto:
	$(PROTOC) --proto_path=proto \
		--go_out=. --go_opt=module=$(PKG) \
		--go-grpc_out=. --go-grpc_opt=module=$(PKG) \
		proto/atlas.proto

tools:
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0

## chaos: a short randomized fault-injection campaign against real processes
chaos: build
	$(BIN)/atlas-chaos --workers 6 --jobs 400 --duration 60s

## chaos-campaign: the long campaign reported in docs/RESULTS.md
chaos-campaign: build
	$(BIN)/atlas-chaos --workers 12 --jobs 4000 --duration 240s \
		--kill-workers --restart-scheduler --duplicate-rpc-rate 0.15 \
		--heartbeat-drop-rate 0.1 --report results/chaos-campaign.txt

## bench-policy: compare placement policies on an identical workload
bench-policy: build
	$(BIN)/atlas-sim policy --workers 200 --jobs 20000 --out results/policy.txt

## bench-scale: scheduler decision throughput as the fleet grows
bench-scale: build
	$(BIN)/atlas-sim scale --jobs 50000 --out results/scale.txt

## bench-overload: admission control under increasing arrival rate
bench-overload: build
	$(BIN)/atlas-sim overload --out results/overload.txt

## bench-fragmentation: heterogeneous fleet, mixed CPU- and memory-heavy jobs
bench-fragmentation: build
	$(BIN)/atlas-sim fragmentation --out results/fragmentation.txt

## experiments: every simulator experiment, into ./results
experiments: bench-policy bench-scale bench-overload bench-fragmentation

## bench: Go microbenchmarks for the scheduler hot path
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/scheduler/... ./benchmarks/...

clean:
	rm -rf $(BIN) results *.db *.db-wal *.db-shm

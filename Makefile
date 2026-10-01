GO            ?= go
GOTOOLCHAIN   ?= local
BIN           := bin
PROTOC        ?= protoc
PKG           := github.com/BruceMoseti/Atlas

export GOTOOLCHAIN

.PHONY: all build test test-race test-integration test-all lint fmt vet proto clean \
        chaos chaos-campaign bench bench-policy bench-scale bench-overload \
        bench-fragmentation experiments figures tools help

## help: list the available targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | awk -F': ' '{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

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
	gofmt -w ./cmd ./internal ./simulator ./chaos ./tests

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
	$(BIN)/atlas-chaos --workers 10 --jobs 3000 --duration 150s --job-duration 1500ms \
		--fault-interval 2s --drain-timeout 240s --restart-scheduler \
		--duplicate-rpc-rate 0.2 --heartbeat-drop-rate 0.1 --renew-drop-rate 0.05 \
		--max-attempts 10 --seed 1 --report results/chaos-campaign.txt

## bench-policy: compare placement policies on an identical workload
bench-policy: build
	$(BIN)/atlas-sim policy --workers 100 --jobs 20000 --seed 1 --out results/policy.txt

## bench-scale: scheduler decision throughput as the fleet grows
bench-scale: build
	$(BIN)/atlas-sim scale --decisions 200000 --seed 1 --out results/scale.txt

## bench-overload: admission control under increasing arrival rate
bench-overload: build
	$(BIN)/atlas-sim overload --jobs 20000 --seed 1 --out results/overload.txt

## bench-fragmentation: heterogeneous fleet, mixed CPU- and memory-heavy jobs
bench-fragmentation: build
	$(BIN)/atlas-sim fragmentation --jobs 20000 --seed 1 --out results/fragmentation.txt

## bench-priority: strict priority versus priority with aging
bench-priority: build
	$(BIN)/atlas-sim priority --jobs 20000 --seed 1 --out results/priority.txt

## bench-failure: worker failure rate versus job turnaround
bench-failure: build
	$(BIN)/atlas-sim failure --jobs 20000 --seed 1 --out results/failure.txt

## experiments: every simulator experiment, into ./results
experiments: bench-policy bench-fragmentation bench-priority bench-overload bench-failure bench-scale

## figures: regenerate docs/images/*.png from ./results (needs matplotlib)
figures:
	python3 scripts/plot_results.py

## bench: Go microbenchmarks for the scheduler hot path
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/scheduler/...

## clean: remove build output and chaos working directories
clean:
	rm -rf $(BIN) results/chaos-*/ *.db *.db-wal *.db-shm

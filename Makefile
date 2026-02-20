default: init

.PHONY: init
init: init/lint init/hooks

.PHONY: init/hooks
init/hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit
	@echo "Git hooks installed."

.PHONY: init/lint init/vulnCheck
init/lint:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest

.PHONY: init/vulnCheck
init/vulnCheck:
	go install golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: audit
audit:
	@echo 'Formatting code...'
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix
	@echo 'Vetting code...'
	go vet ./...
	@echo 'Vulnerability scanning...'
	govulncheck ./...

.PHONY: tidy
tidy:
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy -compat=1.26
	go mod verify

.PHONY: tidy/all
tidy/all:
	go mod tidy
	cd example/simple && go mod tidy && cd ../..

.PHONY: test/integration
test/integration:
	cd integration_test && go test -race -p=1 -v ./...

.PHONY: test/benchmark
test/benchmark:
	cd benchmark/go-pq-cdc-rabbitmq && go test -bench=. -benchmem -v ./...

.PHONY: bench
bench:
	go test -run='^$$' -bench=BenchmarkPipeline -benchmem -count=6 -timeout=600s ./integration_test/... 2>&1 | tee bench-results.txt

.PHONY: bench/install
bench/install:
	go install golang.org/x/perf/cmd/benchstat@latest

# Compare current commit against a baseline commit.
# Usage: make bench/compare BASE=02a0eba
.PHONY: bench/compare
bench/compare:
	@test -n "$(BASE)" || (echo "Usage: make bench/compare BASE=<commit>" && exit 1)
	@echo "=== Running benchmarks on current HEAD ==="
	go test -run='^$$' -bench=BenchmarkPipeline -benchmem -count=6 -timeout=600s ./integration_test/... 2>&1 | tee bench-after.txt
	@echo ""
	@echo "=== Checking out baseline $(BASE) ==="
	git stash --include-untracked -m "bench-compare-stash"
	git checkout $(BASE)
	git checkout stash@{0} -- integration_test/bench_test.go
	@echo "=== Running benchmarks on $(BASE) ==="
	go test -run='^$$' -bench=BenchmarkPipeline -benchmem -count=6 -timeout=600s ./integration_test/... 2>&1 | tee bench-before.txt
	git checkout -
	git stash pop || true
	@echo ""
	@echo "=== Results ==="
	benchstat bench-before.txt bench-after.txt

.PHONY: lint
lint: init/lint
	@echo 'Formatting code...'
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

.PHONY: build
build/linux:
	GOOS=linux CGO_ENABLED=0 GOARCH=amd64 go build -trimpath -a -v ./...

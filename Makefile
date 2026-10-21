.PHONY: run build test test-integration test-race check coverage

COVERAGE_THRESHOLD ?= 90

run:
	go run ./cmd/server

build:
	go build -buildvcs=false -o bin/server ./cmd/server

test:
	go test ./...

test-integration:
	./scripts/test-integration-podman.sh

check:
	go vet ./...
	$(MAKE) coverage

test-race:
	go test -race ./...

coverage:
	./scripts/test-integration-podman.sh -coverprofile=coverage.out
	awk -f scripts/check-coverage.awk coverage.out $(COVERAGE_THRESHOLD)

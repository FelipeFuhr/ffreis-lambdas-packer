SHELL := /usr/bin/env bash
.SHELLFLAGS := -e -o pipefail -c

GO ?= go
GO_IMAGE ?= golang:1.22
CONTAINER_RUNTIME ?= docker

.PHONY: help fmt-check test vet check check-container fmt-check-container test-container vet-container

help:
	@echo "Targets:"
	@echo "  make check                (fmt-check + vet + test)"
	@echo "  make check-container       (containerized fmt-check + vet + test)"
	@echo "  make test|test-container"

fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [[ -n "$$unformatted" ]]; then \
		echo "Formatting required (run: gofmt -w .):"; \
		printf "%s\n" $$unformatted; \
		exit 1; \
	fi

vet:
	$(GO) vet ./...

test:
	$(GO) test ./... -count=1

check: fmt-check vet test

container-run = $(CONTAINER_RUNTIME) run --rm -t \
	-v "$(PWD):/work" -w /work \
	"$(GO_IMAGE)" \
	bash -lc

fmt-check-container:
	$(call container-run,'make fmt-check')

vet-container:
	$(call container-run,'make vet')

test-container:
	$(call container-run,'make test')

check-container:
	$(call container-run,'make check')


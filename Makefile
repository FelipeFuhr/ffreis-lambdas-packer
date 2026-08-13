SHELL := /usr/bin/env bash
.SHELLFLAGS := -e -o pipefail -c

GO ?= go
GO_IMAGE ?= golang:1.22
CONTAINER_RUNTIME ?= docker
GITLEAKS ?= gitleaks
GOVULNCHECK ?= govulncheck

COVERAGE_MIN       ?= 75
MUTATION_PACKAGES  ?= ./internal/...
MUTATION_THRESHOLD ?= 60
FUZZ_PACKAGES      ?= ./...
FUZZ_TIME          ?= 30s

.PHONY: help build-all fuzz fmt-check test vet check check-container fmt-check-container test-container vet-container \
	coverage-gate integration-coverage-gate mutation quality-gates secrets-scan-staged

build-all: ## Compile all Go packages required by the lefthook release tier
	$(GO) build ./...

fuzz: ## Run all Fuzz* targets for FUZZ_TIME each (no-op when none exist)
	@for pkg in $$($(GO) list $(FUZZ_PACKAGES)); do targets=$$($(GO) test -list 'Fuzz.*' "$$pkg" 2>/dev/null | grep '^Fuzz' || true); for target in $$targets; do $(GO) test -run='^$$' -fuzz="^$${target}$$" -fuzztime="$(FUZZ_TIME)" "$$pkg"; done; done

help:
	@echo "Targets:"
	@echo "  make check                (fmt-check + vet + test)"
	@echo "  make check-container       (containerized fmt-check + vet + test)"
	@echo "  make test|test-container"
	@echo "  make coverage-gate|integration-coverage-gate|mutation|quality-gates"

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
	$(GO) test -race -shuffle=on ./... -count=1

check: fmt-check vet test

## coverage-gate: Fail if test coverage falls below COVERAGE_MIN
coverage-gate:
	@tmpfile=$$(mktemp); \
	$(GO) test -coverprofile="$$tmpfile" ./... > /dev/null; \
	pct=$$($(GO) tool cover -func="$$tmpfile" | tail -1 | awk '{print $$3}' | tr -d '%'); \
	echo "Coverage: $${pct}% (min: $(COVERAGE_MIN)%)"; \
	awk -v pct="$$pct" -v min=$(COVERAGE_MIN) 'BEGIN { if (pct+0 < min+0) { print "FAIL: below minimum"; exit 1 } }'; \
	rm -f "$$tmpfile"

## integration-coverage-gate: run //go:build integration tests with coverage; no-op if none exist
integration-coverage-gate:
	@if ! grep -rl '^//go:build integration' --include='*.go' . >/dev/null 2>&1; then \
		echo "No '//go:build integration' files found — skipping integration-coverage-gate."; \
		exit 0; \
	fi; \
	tmpfile=$$(mktemp); \
	$(GO) test -tags=integration -coverprofile="$$tmpfile" ./... > /dev/null; \
	pct=$$($(GO) tool cover -func="$$tmpfile" | tail -1 | awk '{print $$3}' | tr -d '%'); \
	echo "Integration coverage: $${pct}% (min: $(COVERAGE_MIN)%)"; \
	awk -v pct="$$pct" -v min=$(COVERAGE_MIN) 'BEGIN { if (pct+0 < min+0) { print "FAIL: below minimum"; exit 1 } }'; \
	rm -f "$$tmpfile"

## mutation: run mutation testing with gremlins (slow — intended for CI/weekly)
mutation:
	@which gremlins >/dev/null 2>&1 || $(GO) install github.com/go-gremlins/gremlins/cmd/gremlins@latest
	gremlins unleash --threshold-efficacy $(MUTATION_THRESHOLD) $(MUTATION_PACKAGES)

## quality-gates: Full pre-push/pre-ready quality suite
quality-gates:
	@command -v $(GOVULNCHECK) >/dev/null 2>&1 || $(GO) install golang.org/x/vuln/cmd/govulncheck@latest
	$(MAKE) test
	$(MAKE) coverage-gate
	$(GOVULNCHECK) ./...

## secrets-scan-staged: scan staged diff for secrets
secrets-scan-staged:
	@command -v $(GITLEAKS) >/dev/null 2>&1 || (echo "Missing: gitleaks" && exit 1)
	$(GITLEAKS) protect --staged --redact

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


PLATFORM_STANDARDS_SHA ?= 273842219190739c6b462c21331b234271446b13  # v1.10.0
PLATFORM_STANDARDS_RAW ?= https://raw.githubusercontent.com/FelipeFuhr/ffreis-platform-standards

install-act: ## Download pinned act binary into .bin/
	@mkdir -p scripts
	@curl -fsSL "$(PLATFORM_STANDARDS_RAW)/$(PLATFORM_STANDARDS_SHA)/scripts/install_act.sh" \
		-o scripts/install_act.sh && chmod +x scripts/install_act.sh
	@bash ./scripts/install_act.sh

ci-local: ## Run workflows locally via act (GH Actions quota fallback). Args via ARGS=...
	@mkdir -p scripts
	@curl -fsSL "https://raw.githubusercontent.com/FelipeFuhr/ffreis-platform-ci-local/v1.0.0/scripts/run-ci-local.sh" \
		-o scripts/run-ci-local.sh && chmod +x scripts/run-ci-local.sh
	@CI_LOCAL_FINDINGS_REF=v1.0.0 PATH="$(CURDIR)/.bin:$(PATH)" bash ./scripts/run-ci-local.sh $(ARGS)

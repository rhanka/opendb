SHELL := /bin/bash

# OpenDB workflow targets. Mirrors `sentropic/Makefile` style: every target
# has a `## comment` consumed by `make help`. Run targets from this worktree:
#
#   cd /home/antoinefa/src/opendb/.worktrees/feat-milestone-1 && make <target>
#
# All targets are designed to run without requiring per-command permission
# prompts, by wrapping recurring patterns (cargo / git / npm / pg) under
# `Bash(make *)` which is allow-listed in `.claude/settings.json`.

WORKTREE := /home/antoinefa/src/opendb/.worktrees/feat-milestone-1
CARGO    := cargo
GIT      := git -C $(WORKTREE)
NPM      := npm
NODE_BIN := $(WORKTREE)/target/debug/opendb-node

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@echo "OpenDB Makefile — drumbeat-friendly workflow targets"
	@echo ""
	@echo "  Quality:"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; /Quality/ {next} /Build|Test|Lint|Check/ {printf "    \033[32m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  Git workflow:"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; /Git|Commit|Push|Ship|Status/ {printf "    \033[32m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  POC / smoke:"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; /POC|Smoke|Probe|Bench/ {printf "    \033[32m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  DB workflow:"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; /DB|Query|opendb/ {printf "    \033[32m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  Audit:"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; /Audit|sentropic|grep|cloc/ {printf "    \033[32m%-22s\033[0m %s\n", $$1, $$2}'

# ----- Build / Test / Lint / Check ------------------------------------------

.PHONY: build
build: ## Build the full Rust workspace
	cd $(WORKTREE) && $(CARGO) build --workspace

.PHONY: build-release
build-release: ## Build the Rust workspace in release mode
	cd $(WORKTREE) && $(CARGO) build --workspace --release

.PHONY: test
test: ## Run the full cargo test workspace
	cd $(WORKTREE) && $(CARGO) test --workspace

.PHONY: test-storage
test-storage: ## Run cargo tests for opendb-storage only
	cd $(WORKTREE) && $(CARGO) test -p opendb-storage

.PHONY: test-sql
test-sql: ## Run cargo tests for opendb-sql only
	cd $(WORKTREE) && $(CARGO) test -p opendb-sql

.PHONY: test-node
test-node: ## Run cargo tests for opendb-node only
	cd $(WORKTREE) && $(CARGO) test -p opendb-node

.PHONY: fmt
fmt: ## Apply rustfmt to the whole workspace
	cd $(WORKTREE) && $(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Verify rustfmt has nothing to change
	cd $(WORKTREE) && $(CARGO) fmt --all -- --check

.PHONY: clippy
clippy: ## Run cargo clippy with -D warnings on the workspace
	cd $(WORKTREE) && $(CARGO) clippy --workspace --all-targets -- -D warnings

.PHONY: lint
lint: fmt-check clippy ## fmt-check + clippy

.PHONY: check-ts
check-ts: ## TypeScript typecheck (npm run check:ts)
	cd $(WORKTREE) && $(NPM) run check:ts

.PHONY: check-no-python
check-no-python: ## Verify no Python file leaked into the repo
	cd $(WORKTREE) && $(NPM) run check:no-python

.PHONY: check-manifests
check-manifests: ## Validate Kubernetes manifests
	cd $(WORKTREE) && $(NPM) run check:manifests

.PHONY: vitest
vitest: ## Run all vitest suites (npm test)
	cd $(WORKTREE) && $(NPM) test

.PHONY: check
check: lint test check-ts check-no-python check-manifests vitest ## Full check pipeline (lint + tests + npm checks + vitest)

# ----- Git workflow ---------------------------------------------------------

.PHONY: status
status: ## Show git status + recent log
	$(GIT) status --short
	@echo "---"
	$(GIT) log --oneline -10

.PHONY: ai-grep-check
ai-grep-check: ## Count commits matching anthropic/claude/🤖 (must stay ≤1 baseline)
	@$(GIT) log origin/main --grep="anthropic\|claude\|🤖" -i --oneline | wc -l

.PHONY: commit
commit: ## Stage all + commit with MSG="..." (use ship to also push)
	@if [ -z "$(MSG)" ]; then echo "MSG is required: make commit MSG=\"feat: ...\""; exit 1; fi
	$(GIT) add -A
	$(GIT) -c commit.gpgsign=false commit -m "$(MSG)"

.PHONY: push
push: ## Push HEAD to origin/main
	$(GIT) push origin HEAD:main

.PHONY: ship
ship: ## Stage + commit + push in one shot with MSG="..."
	@if [ -z "$(MSG)" ]; then echo "MSG is required: make ship MSG=\"feat: ...\""; exit 1; fi
	$(GIT) add -A
	$(GIT) -c commit.gpgsign=false commit -m "$(MSG)"
	$(GIT) push origin HEAD:main

.PHONY: diff
diff: ## Show uncommitted diff
	$(GIT) diff

# ----- POC smoke / benches --------------------------------------------------

.PHONY: smoke
smoke: ## Run the sentropic POC smoke (POC = end-to-end Drizzle probe matrix)
	cd $(WORKTREE) && $(NPM) run poc:sentropic:smoke

.PHONY: smoke-real
smoke-real: ## Sprint 15.E corrective: rejoue 8 vraies requêtes sentropic
	cd $(WORKTREE) && $(NPM) run poc:sentropic:real

.PHONY: poc-migrate
poc-migrate: ## Sprint 18.A: rejoue les 27 migrations Drizzle sentropic contre opendb-node
	cd $(WORKTREE) && $(NPM) run poc:sentropic:migrate

.PHONY: poc-seed
poc-seed: ## Sprint 18.B: migrations + seed minimal Drizzle (workspaces/orgs/folders/initiatives)
	cd $(WORKTREE) && $(NPM) run poc:sentropic:seed

.PHONY: poc-http
poc-http: ## Sprint 18.C: HTTP route /api/folders bout-en-bout sur opendb-node
	cd $(WORKTREE) && $(NPM) run poc:sentropic:http

.PHONY: poc-image
poc-image: poc-musl ## Sprint 19.C.2: docker build static-musl alpine image (opendb-node:poc-local)
	cd $(WORKTREE) && docker build -t opendb-node:poc-local -f Dockerfile.alpine .

.PHONY: poc-bench
poc-bench: ## Sprint 20: side-by-side latency bench opendb-node vs postgres:16-alpine
	cd $(WORKTREE) && $(NPM) run poc:sentropic:bench

.PHONY: poc-bench-concurrent
poc-bench-concurrent: ## Phase B prep: multi-client concurrent INSERT+SELECT bench opendb vs PG
	cd $(WORKTREE) && $(NPM) run poc:sentropic:bench-concurrent

.PHONY: poc-musl
poc-musl: ## Sprint 19.C.1: cargo build --release --target x86_64-unknown-linux-musl (no docker)
	cd $(WORKTREE) && CC=musl-gcc $(CARGO) build --release --target x86_64-unknown-linux-musl -p opendb-node

.PHONY: smoke-k3s
smoke-k3s: ## Run the k3s cluster smoke (non-destructive default)
	cd $(WORKTREE) && $(NPM) run smoke:k3s

.PHONY: bench-jsonb
bench-jsonb: ## JSONB throughput bench
	cd $(WORKTREE) && $(NPM) run bench:jsonb -- --rows $${ROWS:-100}

.PHONY: bench-alter
bench-alter: ## ALTER TABLE bench
	cd $(WORKTREE) && $(NPM) run bench:alter -- --rows $${ROWS:-50}

.PHONY: bench-fk
bench-fk: ## FK insert/delete bench
	cd $(WORKTREE) && $(NPM) run bench:fk -- --rows $${ROWS:-50}

# ----- DB workflow (local opendb-node for ad-hoc queries) -------------------

OPENDB_PGWIRE_PORT ?= 25432
OPENDB_HEALTH_PORT ?= 28080
OPENDB_DATA_DIR    ?= $(WORKTREE)/tmp/opendb-local

.PHONY: db-up
db-up: build ## Spawn a local opendb-node on $(OPENDB_PGWIRE_PORT) (background)
	@mkdir -p $(OPENDB_DATA_DIR)
	@pkill -f 'opendb-node.*--pgwire-addr=127.0.0.1:$(OPENDB_PGWIRE_PORT)' 2>/dev/null || true
	$(NODE_BIN) --node-id 1 \
	  --data-dir $(OPENDB_DATA_DIR) \
	  --pgwire-addr 127.0.0.1:$(OPENDB_PGWIRE_PORT) \
	  --health-addr 127.0.0.1:$(OPENDB_HEALTH_PORT) > $(WORKTREE)/tmp/opendb-local.log 2>&1 &
	@sleep 1
	@echo "opendb-node up on pgwire 127.0.0.1:$(OPENDB_PGWIRE_PORT) (logs: $(WORKTREE)/tmp/opendb-local.log)"

.PHONY: db-down
db-down: ## Stop the local opendb-node started by db-up
	@pkill -f 'opendb-node.*--pgwire-addr=127.0.0.1:$(OPENDB_PGWIRE_PORT)' 2>/dev/null && echo "stopped" || echo "no opendb-node running on $(OPENDB_PGWIRE_PORT)"

.PHONY: db-reset
db-reset: db-down ## Stop opendb-node and wipe its data dir
	@rm -rf $(OPENDB_DATA_DIR)
	@echo "data dir $(OPENDB_DATA_DIR) cleared"

.PHONY: db-query
db-query: ## Run a one-shot SQL query against the local opendb-node: SQL="SELECT * FROM t"
	@if [ -z "$(SQL)" ]; then echo "SQL is required: make db-query SQL=\"SELECT 1\""; exit 1; fi
	cd $(WORKTREE) && OPENDB_PGWIRE_HOST=127.0.0.1 OPENDB_PGWIRE_PORT=$(OPENDB_PGWIRE_PORT) tsx tools/pgwire-exec.ts "$(SQL)"

.PHONY: db-status
db-status: ## Check whether the local opendb-node is up
	@curl -fsS http://127.0.0.1:$(OPENDB_HEALTH_PORT)/healthz && echo "" && echo "opendb-node health OK on $(OPENDB_HEALTH_PORT)" || echo "opendb-node not reachable on $(OPENDB_HEALTH_PORT)"

# ----- Audit (cross-repo grep helpers) --------------------------------------

SENTROPIC_ROOT ?= /home/antoinefa/src/sentropic/api

.PHONY: audit-sentropic-tables
audit-sentropic-tables: ## Count Drizzle pgTable() declarations in sentropic
	@grep -cE "pgTable\(" $(SENTROPIC_ROOT)/src/db/schema.ts

.PHONY: audit-sentropic-types
audit-sentropic-types: ## Histogram of column types declared in sentropic migrations
	@grep -ohE '\b(text|timestamp|jsonb|boolean|integer|bigint|uuid|varchar|numeric|serial|date|interval|bytea)\b' $(SENTROPIC_ROOT)/drizzle/*.sql | sort | uniq -c | sort -rn

.PHONY: audit-sentropic-verbs
audit-sentropic-verbs: ## Drizzle verb usage histogram in sentropic src/
	@printf "%-20s %5d\n" ".select(" "$$(grep -rE '\.select\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" ".update(" "$$(grep -rE '\.update\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" ".delete(" "$$(grep -rE '\.delete\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" ".insert(" "$$(grep -rE '\.insert\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" ".where(and(" "$$(grep -rE '\.where\(and\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" ".where(or(" "$$(grep -rE '\.where\(or\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" ".where(eq(" "$$(grep -rE '\.where\(eq\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" ".returning(" "$$(grep -rE '\.returning\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" "db.transaction(" "$$(grep -rE 'db\.transaction\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"
	@printf "%-20s %5d\n" "groupBy/agg" "$$(grep -rE '\.groupBy\(|\.having\(|count\(|sum\(|max\(|min\(|avg\(' $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | wc -l)"

.PHONY: audit-sentropic-grep
audit-sentropic-grep: ## Generic grep over sentropic src: PATTERN="..."
	@if [ -z "$(PATTERN)" ]; then echo "PATTERN is required: make audit-sentropic-grep PATTERN='SELECT'"; exit 1; fi
	@grep -rE "$(PATTERN)" $(SENTROPIC_ROOT)/src --include='*.ts' 2>/dev/null | head -50

.PHONY: cloc
cloc: ## Count lines of code in the opendb workspace
	@cd $(WORKTREE) && cloc --vcs=git crates tools tests docs 2>/dev/null || echo "cloc not installed"

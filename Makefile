PY := .venv/bin/python
PIP := .venv/bin/pip

# Local dev database. `make up` (Docker) and `make db-local-up` (a throwaway
# Homebrew cluster in ./.pgdata) both listen here, so nothing else changes.
export LEDGER_DATABASE_URL ?= postgresql://ledger@127.0.0.1:55432/ledger
export LEDGER_TEST_DATABASE_URL ?= postgresql://ledger@127.0.0.1:55432/ledger_test

PG_BIN := /opt/homebrew/opt/postgresql@16/bin

.PHONY: help venv up down migrate seed run test test-fast loadtest chaos \
        reconcile integrity db-local-up db-local-down clean

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | expand -t28

venv: ## Create .venv and install dependencies
	python3.12 -m venv .venv
	$(PIP) install -q -e '.[dev]'

up: ## Start Postgres 16 in Docker
	docker compose up -d --wait
	docker compose exec -T db psql -U ledger -d postgres -c \
	  "SELECT 'CREATE DATABASE ledger_test' FROM (SELECT 1) s WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname='ledger_test')\gexec"

down: ## Stop Postgres and delete its volume
	docker compose down -v

db-local-up: ## Start a throwaway Homebrew Postgres 16 cluster in ./.pgdata (no Docker)
	@test -d .pgdata || (LC_ALL=en_US.UTF-8 $(PG_BIN)/initdb -D .pgdata -U ledger \
	  --encoding=UTF8 --locale=en_US.UTF-8 >/dev/null && echo "initialised .pgdata")
	@$(PG_BIN)/pg_ctl -D .pgdata -l .pgdata/server.log \
	  -o "-p 55432 -k /tmp -c listen_addresses=127.0.0.1" start
	@sleep 1
	@$(PG_BIN)/createdb -h 127.0.0.1 -p 55432 -U ledger ledger 2>/dev/null || true
	@$(PG_BIN)/createdb -h 127.0.0.1 -p 55432 -U ledger ledger_test 2>/dev/null || true

db-local-down: ## Stop the Homebrew cluster
	@$(PG_BIN)/pg_ctl -D .pgdata stop || true

migrate: ## Apply pending migrations to both databases
	$(PY) -m scripts.migrate
	LEDGER_DATABASE_URL=$(LEDGER_TEST_DATABASE_URL) $(PY) -m scripts.migrate

seed: ## Create system accounts and the opening funding transaction
	$(PY) -m scripts.seed

run: ## Serve the API on :8000
	.venv/bin/uvicorn ledger.api:app --host 127.0.0.1 --port 8000 --reload

test: ## Full test suite
	$(PY) -m pytest

test-fast: ## Skip the slow concurrency and chaos tests
	$(PY) -m pytest -m 'not slow'

loadtest: ## Hot-account benchmark, writes docs/hot-account-benchmark.png
	$(PY) -m scripts.loadtest

chaos: ## Randomized operations with process kills
	$(PY) -m scripts.chaos

reconcile: ## Print the reconciliation report
	$(PY) -m scripts.reconcile

clean:
	rm -rf .pytest_cache .hypothesis **/__pycache__

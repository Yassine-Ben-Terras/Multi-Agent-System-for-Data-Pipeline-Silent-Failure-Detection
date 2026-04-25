# ─────────────────────────────────────────────────────────────
# MAS Pipeline Sentinel — Makefile
# ─────────────────────────────────────────────────────────────

.PHONY: help up down logs build ps topics venv install \
        test test-unit test-cov lint clean \
        smoke smoke-zero smoke-drift smoke-combined

# ── Help ─────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  MAS Pipeline Sentinel — Developer Commands"
	@echo "  ─────────────────────────────────────────"
	@echo "  Infrastructure"
	@echo "    make up              Start full stack (shadow mode)"
	@echo "    make down            Stop all containers"
	@echo "    make build           Rebuild all agent images"
	@echo "    make logs            Tail all container logs"
	@echo "    make ps              Show container status"
	@echo ""
	@echo "  Agent Modes"
	@echo "    make up              AGENT_MODE=shadow (default)"
	@echo "    AGENT_MODE=alert_only make up"
	@echo "    AGENT_MODE=supervised make up"
	@echo "    AGENT_MODE=full_autonomy make up"
	@echo ""
	@echo "  Testing"
	@echo "    make test            Run all unit tests"
	@echo "    make test-unit       Run unit tests only"
	@echo "    make test-cov        Run tests with coverage report"
	@echo "    make smoke           End-to-end smoke test (row count anomaly)"
	@echo "    make smoke-zero      Smoke test: zero rows scenario"
	@echo "    make smoke-drift     Smoke test: schema drift scenario"
	@echo "    make smoke-combined  Smoke test: schema drift + row count"
	@echo ""
	@echo "  Dev"
	@echo "    make venv            Create Python virtual environment"
	@echo "    make install         Install Python dependencies"
	@echo "    make lint            Run ruff linter"
	@echo "    make clean           Remove build artifacts"
	@echo ""

# ── Infrastructure ───────────────────────────────────────────
up:
	docker compose up -d
	@echo ""
	@echo "✅ Stack is up!"
	@echo "   Kafka UI  → http://localhost:8080"
	@echo "   Postgres  → localhost:5432 (mas_user / changeme)"
	@echo "   Mode      → $${AGENT_MODE:-shadow}"
	@echo ""
	@echo "   Tip: docker compose logs -f orchestrator"

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps

# ── Python ───────────────────────────────────────────────────
venv:
	python -m venv .venv
	@echo "✅ Venv created → activate with: source .venv/bin/activate (Linux/Mac)"
	@echo "                                  .venv\\Scripts\\activate (Windows)"

install:
	pip install -r requirements.txt

# ── Tests ────────────────────────────────────────────────────
test:
	python -m pytest tests/ -v

test-unit:
	python -m pytest tests/unit/ -v

test-cov:
	python -m pytest tests/ --cov=. --cov-report=html --cov-report=term-missing
	@echo "📊 Coverage report → htmlcov/index.html"

# ── Smoke tests ──────────────────────────────────────────────
smoke:
	@echo "Running smoke test: row_count_anomaly"
	python scripts/smoke_test.py --scenario row_count_anomaly

smoke-zero:
	@echo "Running smoke test: zero_rows"
	python scripts/smoke_test.py --scenario zero_rows

smoke-drift:
	@echo "Running smoke test: schema_drift"
	python scripts/smoke_test.py --scenario schema_drift

smoke-combined:
	@echo "Running smoke test: combined (schema_drift + row_count)"
	python scripts/smoke_test.py --scenario combined

# ── Code Quality ─────────────────────────────────────────────
lint:
	ruff check . --fix

# ── Cleanup ──────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf htmlcov/ .coverage .pytest_cache/ dist/ build/ *.egg-info/
	@echo "✅ Cleaned"

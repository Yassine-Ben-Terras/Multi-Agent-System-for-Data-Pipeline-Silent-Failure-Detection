# ─────────────────────────────────────────────────────────────
# MAS Pipeline Sentinel — Makefile
# ─────────────────────────────────────────────────────────────

.PHONY: help up down logs topics venv install test test-unit test-cov shadow-mode lint clean

# ── Help ─────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  MAS Pipeline Sentinel — Developer Commands"
	@echo "  ─────────────────────────────────────────"
	@echo "  make up           Start full local stack (Kafka + Postgres)"
	@echo "  make down         Stop all containers"
	@echo "  make logs         Tail all container logs"
	@echo "  make topics       Create Kafka topics"
	@echo "  make venv         Create Python virtual environment"
	@echo "  make install      Install Python dependencies"
	@echo "  make test         Run all tests"
	@echo "  make test-unit    Run unit tests only"
	@echo "  make test-cov     Run tests with coverage report"
	@echo "  make shadow-mode  Start agents in shadow (observe-only) mode"
	@echo "  make lint         Run ruff linter"
	@echo "  make clean        Remove build artifacts"
	@echo ""

# ── Infrastructure ───────────────────────────────────────────
up:
	docker compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 10
	@make topics
	@echo ""
	@echo "✅ Stack is up!"
	@echo "   Kafka UI → http://localhost:8080"
	@echo "   Postgres → localhost:5432 (mas_user / changeme)"

down:
	docker compose down

logs:
	docker compose logs -f

topics:
	@echo "Creating Kafka topics..."
	@docker exec mas_kafka bash /infra/kafka/create_topics.sh 2>/dev/null || \
	 docker exec mas_kafka kafka-topics --bootstrap-server localhost:9092 \
	   --create --if-not-exists --topic pipeline.signals.raw --partitions 3 --replication-factor 1 && \
	 docker exec mas_kafka kafka-topics --bootstrap-server localhost:9092 \
	   --create --if-not-exists --topic agents.anomalies --partitions 3 --replication-factor 1 && \
	 docker exec mas_kafka kafka-topics --bootstrap-server localhost:9092 \
	   --create --if-not-exists --topic agents.confirmed_incidents --partitions 3 --replication-factor 1 && \
	 docker exec mas_kafka kafka-topics --bootstrap-server localhost:9092 \
	   --create --if-not-exists --topic agents.actions_taken --partitions 3 --replication-factor 1 && \
	 docker exec mas_kafka kafka-topics --bootstrap-server localhost:9092 \
	   --create --if-not-exists --topic agents.heartbeats --partitions 3 --replication-factor 1
	@echo "✅ Topics ready"

# ── Python ───────────────────────────────────────────────────
venv:
	python -m venv .venv
	@echo "✅ Venv created → activate with: source .venv/bin/activate"

install:
	pip install -r requirements.txt

# ── Tests ────────────────────────────────────────────────────
test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-cov:
	pytest tests/ --cov=. --cov-report=html --cov-report=term-missing
	@echo "📊 Coverage report → htmlcov/index.html"

# ── Agents ───────────────────────────────────────────────────
shadow-mode:
	@echo "Starting agents in SHADOW MODE (observe only, no actions)..."
	AGENT_MODE=shadow python -m agents.orchestrator.main &
	AGENT_MODE=shadow python -m agents.ingestion_monitor.main &
	AGENT_MODE=shadow python -m agents.schema_watcher.main &
	AGENT_MODE=shadow python -m agents.quality_auditor.main &
	AGENT_MODE=shadow python -m agents.lineage_impact.main &
	@echo "✅ Agents running in shadow mode. Check logs for signals."

# ── Code Quality ─────────────────────────────────────────────
lint:
	ruff check . --fix

# ── Cleanup ──────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf htmlcov/ .coverage .pytest_cache/ dist/ build/ *.egg-info/
	@echo "✅ Cleaned"

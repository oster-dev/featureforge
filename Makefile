SHELL := /bin/zsh

PYTHON ?= python
PIP ?= $(PYTHON) -m pip
PYTEST ?= $(PYTHON) -m pytest
RUFF ?= $(PYTHON) -m ruff

OUTPUT_DIR ?= output_e2e
OFFLINE_STORE_DIR ?= output/offline_store
CONFIG ?= configs/synthetic_data.yaml
REPO ?= feature_repo
BACKFILL_START ?= 2026-03-20
BACKFILL_END ?= 2026-03-25
WINDOW_DAYS ?= 7
USER_ID ?= user_000290
TOP_K ?= 5

.PHONY: help setup install lint format check test test-unit test-integration \
        validate clean docker-config docker-up docker-down \
        e2e e2e-no-skip e2e-clean e2e-clean-all \
        materialize-incremental demo

help:
	@echo "FeatureForge development commands:"
	@echo "  make setup                    Install project and development dependencies"
	@echo "  make install                  Install the project in editable mode"
	@echo "  make lint                     Run Ruff lint checks across the repository"
	@echo "  make format                   Format and apply safe Ruff fixes repository-wide"
	@echo "  make check                    Run lint and the complete test suite"
	@echo "  make test                     Run the complete test suite"
	@echo "  make test-unit                Run unit tests"
	@echo "  make test-integration         Run integration tests"
	@echo "  make validate                 Validate project dependencies and package import"
	@echo "  make docker-config            Validate Docker Compose configuration"
	@echo "  make docker-up                Start local infrastructure"
	@echo "  make docker-down              Stop local infrastructure"
	@echo "  make clean                    Remove local caches and generated output"
	@echo ""
	@echo "End-to-end orchestration:"
	@echo "  make e2e                      Run generate → backfill → full materialization"
	@echo "  make e2e-no-skip              Run e2e including incremental materialization"
	@echo "  make e2e-clean                Clean run artifacts, then run e2e"
	@echo "  make materialize-incremental  Materialize current features to now (UTC)"
	@echo "  make demo                     Run e2e, online lookup, and ranking demo"
	@echo ""
	@echo "Storage contract:"
	@echo "  OUTPUT_DIR                    Run artifacts such as source data and manifests"
	@echo "  OFFLINE_STORE_DIR             Canonical feature store read by Feast FileSources"
	@echo ""
	@echo "Examples:"
	@echo "  make e2e BACKFILL_START=2026-03-20 BACKFILL_END=2026-03-25"
	@echo "  make demo OUTPUT_DIR=output_demo OFFLINE_STORE_DIR=output/offline_store USER_ID=user_000290 TOP_K=5"

setup:
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

install:
	$(PIP) install -e ".[dev]"

lint:
	$(RUFF) check .

format:
	$(RUFF) format .
	$(RUFF) check --fix .

check: lint test

test:
	$(PYTEST)

test-unit:
	$(PYTEST) tests/unit

test-integration:
	$(PYTEST) tests/integration

validate:
	$(PYTHON) -m pip check
	$(PYTHON) -c "import featureforge; print('featureforge import OK')"

docker-config:
	docker compose config

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -prune -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -prune -exec rm -rf {} +
	find . -type d -name "*.egg-info" -prune -exec rm -rf {} +
	rm -rf output output_e2e

e2e:
	$(PYTHON) scripts/run_end_to_end.py \
		--config $(CONFIG) \
		--output-dir $(OUTPUT_DIR) \
		--offline-store-dir $(OFFLINE_STORE_DIR) \
		--backfill-start $(BACKFILL_START) \
		--backfill-end $(BACKFILL_END) \
		--window-days $(WINDOW_DAYS) \
		--repo $(REPO) \
		--skip-incremental

e2e-no-skip:
	$(PYTHON) scripts/run_end_to_end.py \
		--config $(CONFIG) \
		--output-dir $(OUTPUT_DIR) \
		--offline-store-dir $(OFFLINE_STORE_DIR) \
		--backfill-start $(BACKFILL_START) \
		--backfill-end $(BACKFILL_END) \
		--window-days $(WINDOW_DAYS) \
		--repo $(REPO)

e2e-clean: e2e-clean-all e2e

e2e-clean-all:
	rm -rf $(OUTPUT_DIR)

materialize-incremental:
	$(PYTHON) -c "from datetime import UTC, datetime; print(datetime.now(UTC).isoformat())" | \
		xargs -I {} featureforge materialize-incremental \
			--repo $(REPO) \
			--end-time {} \
			--manifest-output $(OUTPUT_DIR)

demo: e2e
	@echo ""
	@echo "Running Feast online feature lookup demo for $(USER_ID)..."
	$(PYTHON) feature_repo/online_lookup_demo.py --user-id $(USER_ID)
	@echo ""
	@echo "Running Feast personalization ranking demo for $(USER_ID)..."
	$(PYTHON) feature_repo/personalization_demo.py \
		--user-id $(USER_ID) \
		--top-k $(TOP_K)
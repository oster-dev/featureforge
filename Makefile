SHELL := /bin/zsh


PYTHON ?= python
PIP ?= $(PYTHON) -m pip
PYTEST ?= $(PYTHON) -m pytest
RUFF ?= $(PYTHON) -m ruff


.PHONY: help setup install lint format check test test-unit test-integration \
        validate clean docker-config docker-up docker-down \
        e2e e2e-no-skip e2e-clean materialize-incremental e2e-clean-all


help:
	@echo "FeatureForge development commands:"
	@echo "  make setup             Install the project and development dependencies"
	@echo "  make install           Install the project in editable mode"
	@echo "  make lint              Run Ruff lint checks"
	@echo "  make format            Format Python files with Ruff"
	@echo "  make check             Run lint and tests"
	@echo "  make test              Run the complete test suite"
	@echo "  make test-unit         Run unit tests"
	@echo "  make test-integration  Run integration tests"
	@echo "  make validate          Validate the project configuration"
	@echo "  make docker-config     Validate Docker Compose configuration"
	@echo "  make docker-up         Start local infrastructure"
	@echo "  make docker-down       Stop local infrastructure"
	@echo "  make clean             Remove generated local artifacts"
	@echo ""
	@echo "End-to-end orchestration:"
	@echo "  make e2e                 Run end-to-end pipeline (skip incremental)"
	@echo "  make e2e-no-skip         Run end-to-end pipeline (with incremental)"
	@echo "  make e2e-clean           Clean outputs and run end-to-end pipeline"
	@echo "  make materialize-incremental Run incremental materialization to now"


setup:
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"


install:
	$(PIP) install -e ".[dev]"


lint:
	$(RUFF) check src tests


format:
	$(RUFF) format src tests
	$(RUFF) check --fix src tests


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


# End-to-end orchestration

OUTPUT_DIR ?= output_e2e
CONFIG ?= configs/synthetic_data.yaml
REPO ?= feature_repo
BACKFILL_START ?= 2026-03-20
BACKFILL_END ?= 2026-03-25
WINDOW_DAYS ?= 7

e2e:
	$(PYTHON) scripts/run_end_to_end.py \
		--config $(CONFIG) \
		--output-dir $(OUTPUT_DIR) \
		--backfill-start $(BACKFILL_START) \
		--backfill-end $(BACKFILL_END) \
		--window-days $(WINDOW_DAYS) \
		--repo $(REPO) \
		--skip-incremental

e2e-no-skip:
	$(PYTHON) scripts/run_end_to_end.py \
		--config $(CONFIG) \
		--output-dir $(OUTPUT_DIR) \
		--backfill-start $(BACKFILL_START) \
		--backfill-end $(BACKFILL_END) \
		--window-days $(WINDOW_DAYS) \
		--repo $(REPO)

e2e-clean: e2e-clean-all e2e

e2e-clean-all: clean
	rm -rf $(OUTPUT_DIR) output

materialize-incremental:
	$(PYTHON) -c "from datetime import UTC, datetime; print(datetime.now(UTC).isoformat())" | \
		xargs -I {} featureforge materialize-incremental \
			--repo $(REPO) \
			--end-time {} \
			--manifest-output $(OUTPUT_DIR)
SHELL := /bin/bash

PYTHON ?= python
PIP ?= $(PYTHON) -m pip
PYTEST ?= $(PYTHON) -m pytest
RUFF ?= $(PYTHON) -m ruff

EXTRAS ?= dev,feature-store,spark,streaming
OUTPUT_DIR ?= output_e2e
OFFLINE_STORE_DIR ?= output/offline_store
CONFIG ?= configs/synthetic_data.yaml
REPO ?= feature_repo
SPECS ?= feature_repo/specs
BACKFILL_START ?= 2026-03-20
BACKFILL_END ?= 2026-03-25
WINDOW_DAYS ?= 7
USER_ID ?= user_000290
TOP_K ?= 5
FRESHNESS_REFERENCE_TIME ?= $(BACKFILL_END)T00:00:00+00:00
FRESHNESS_MAX_LAG_HOURS ?= 24
BENCH_BACKEND ?= redis

.PHONY: help setup install lint format check test test-unit test-integration \
        validate validate-specs test-spec test-streaming clean \
        docker-config docker-up docker-down \
        check-freshness e2e e2e-no-skip e2e-clean e2e-clean-all \
        materialize-incremental demo bench-compare bench-smoke bench-record quickstart \
        quickstart-failure

help:
	@echo "FeatureForge development commands:"
	@echo "  make setup                    Install project and all development dependencies"
	@echo "  make install                  Install the project in editable mode (same extras)"
	@echo "  make lint                     Run Ruff format check and lint (same as CI)"
	@echo "  make format                   Format and apply safe Ruff fixes repository-wide"
	@echo "  make check                    Run lint and the tests without infrastructure (same as CI)"
	@echo "  make test                     Run the tests without infrastructure"
	@echo "  make test-unit                Run unit tests"
	@echo "  make test-integration         Run all tests in tests/integration (needs Docker services)"
	@echo "  make test-streaming           Rebuild the stack and run the Flink integration modules as CI does"
	@echo "  make validate                 Validate project dependencies and package import"
	@echo "  make validate-specs           Validate every feature spec in $(SPECS)"
	@echo "  make test-spec                Validate specs, then run the spec and golden-fixture unit tests"
	@echo "  make docker-config            Validate Docker Compose configuration"
	@echo "  make docker-up                Start local infrastructure"
	@echo "  make docker-down              Stop local infrastructure"
	@echo "  make clean                    Remove local caches and generated output"
	@echo ""
	@echo "Benchmarks:"
	@echo "  make bench-compare BASE=a.json NEW=b.json   Compare two benchmark reports (COMPARE_ARGS for options)"
	@echo "  make bench-smoke [BENCH_BACKEND=cassandra]   Check that the benchmark tool and the backend work (about 1 minute)"
	@echo "  make bench-record [BENCH_BACKEND=cassandra]  Record a full benchmark (about 20 minutes, clean tree, only the backend services)"
	@echo ""
	@echo "Reliability checks:"
	@echo "  make check-freshness          Validate canonical offline feature freshness"
	@echo ""
	@echo "End-to-end orchestration:"
	@echo "  make e2e                      Run generate -> backfill -> full materialization"
	@echo "  make e2e-no-skip              Run e2e including incremental materialization"
	@echo "  make e2e-clean                Clean run artifacts, then run e2e"
	@echo "  make materialize-incremental  Materialize current features to now (UTC)"
	@echo "  make demo                     Run e2e, online lookup, and ranking demo"
	@echo "  make quickstart               Start Redis and the feature service, write three demo values, read them"
	@echo "  make quickstart-failure       Same, then stop Redis and read again (backend_error)"
	@echo ""
	@echo "Storage and freshness contract:"
	@echo "  OUTPUT_DIR                    Run artifacts such as source data and manifests"
	@echo "  OFFLINE_STORE_DIR             Canonical feature store read by Feast FileSources"
	@echo "  FRESHNESS_REFERENCE_TIME      UTC reference time for freshness checks"
	@echo "  FRESHNESS_MAX_LAG_HOURS       Maximum allowed latest-partition lag"
	@echo ""
	@echo "Examples:"
	@echo "  make check-freshness"
	@echo "  make check-freshness FRESHNESS_REFERENCE_TIME=2026-03-25T00:00:00+00:00"
	@echo "  make e2e BACKFILL_START=2026-03-20 BACKFILL_END=2026-03-25"
	@echo "  make demo OUTPUT_DIR=output_demo OFFLINE_STORE_DIR=output/offline_store USER_ID=user_000290 TOP_K=5"

setup:
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[$(EXTRAS)]"

install:
	$(PIP) install -e ".[$(EXTRAS)]"

lint:
	$(RUFF) format --check .
	$(RUFF) check .

format:
	$(RUFF) format .
	$(RUFF) check --fix .

check: lint test

test:
	$(PYTEST) -m "not integration"

test-unit:
	$(PYTEST) tests/unit

test-integration:
	$(PYTEST) tests/integration

test-streaming:
	docker compose up -d --build
	bash scripts/ci/run_flink_integration.sh

validate:
	$(PYTHON) -m pip check
	$(PYTHON) -c "import featureforge; print('featureforge import OK')"

validate-specs:
	$(PYTHON) -m featureforge.spec.loader $(SPECS)/*.yaml

test-spec: validate-specs
	$(PYTEST) \
		tests/unit/test_spec_model.py \
		tests/unit/test_spec_reference.py \
		tests/unit/test_spec_golden_parity.py \
		tests/unit/test_spec_flink_plan.py \
		tests/unit/test_streaming_user_skip_rate_golden.py

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

check-freshness:
	featureforge check-freshness \
		--reference-time $(FRESHNESS_REFERENCE_TIME) \
		--max-lag-hours $(FRESHNESS_MAX_LAG_HOURS)

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

demo: e2e check-freshness
	@echo ""
	@echo "Running Feast online feature lookup demo for $(USER_ID)..."
	$(PYTHON) feature_repo/online_lookup_demo.py --user-id $(USER_ID)
	@echo ""
	@echo "Running Feast personalization ranking demo for $(USER_ID)..."
	$(PYTHON) feature_repo/personalization_demo.py \
		--user-id $(USER_ID) \
		--top-k $(TOP_K)

bench-compare:
	@test -n "$(BASE)" -a -n "$(NEW)" || { echo "usage: make bench-compare BASE=a.json NEW=b.json"; exit 2; }
	$(PYTHON) scripts/compare_benchmarks.py $(BASE) $(NEW) $(COMPARE_ARGS)

bench-smoke:
	$(PYTHON) scripts/run_serving_benchmark.py smoke --backend $(BENCH_BACKEND)

bench-record:
	$(PYTHON) scripts/run_serving_benchmark.py record --backend $(BENCH_BACKEND)

quickstart:
	docker compose up -d --build --wait redis feature-service
	$(PYTHON) scripts/quickstart.py

quickstart-failure:
	docker compose up -d --build --wait redis feature-service
	$(PYTHON) scripts/quickstart.py --with-failure

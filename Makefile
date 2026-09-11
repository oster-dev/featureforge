SHELL := /bin/zsh

PYTHON ?= python
PIP ?= $(PYTHON) -m pip
PYTEST ?= $(PYTHON) -m pytest
RUFF ?= $(PYTHON) -m ruff

.PHONY: help setup install lint format check test test-unit test-integration \
        validate clean docker-config docker-up docker-down

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
# FeatureForge

FeatureForge is a production-inspired feature platform for reproducible offline
training data and low-latency online ML feature serving.

## Project Goal

FeatureForge provides trusted, versioned, point-in-time-correct features for
both offline model training and online inference.

The project is designed as a portfolio proof for Data Infrastructure,
Feature Infrastructure, and ML Platform Engineering.

## Core Architecture

- Python for platform and pipeline code
- PySpark for batch feature transformations
- Amazon S3 and Parquet as the offline storage profile
- Feast for feature definitions, historical retrieval, and materialization
- Redis as the local online feature store
- DynamoDB as the documented AWS online-store alternative
- pytest for testing
- Ruff for linting and formatting
- GitHub Actions for continuous integration

## Current Status

The project is currently in Day 0 foundation setup.

Completed:

- Repository structure
- Python package configuration
- Development environment
- Linting and testing setup
- Docker Compose Redis service
- Initial package smoke test

Next:

1. Generate deterministic synthetic behavioral data.
2. Build PySpark feature transformations.
3. Integrate Feast feature definitions.
4. Implement point-in-time historical retrieval.
5. Materialize features into Redis.
6. Add data-quality and freshness checks.
7. Document the AWS production profile.

## Development Setup

```bash
git clone <repository-url>
cd featureforge

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Validation

```bash
make validate
make lint
make test
make docker-config
```

## Local Infrastructure

Start Redis:

```bash
make docker-up
```

Check Redis:

```bash
docker exec featureforge-redis redis-cli ping
```

Expected output:

```text
PONG
```

Stop local infrastructure:

```bash
make docker-down
```

## Project Scope

Version 1 focuses on:

- Event-time-aware batch feature computation
- Offline and online feature separation
- Point-in-time-correct historical retrieval
- Reproducible backfills
- Online materialization
- Data-quality validation
- Freshness monitoring
- Tests, CI, and operational documentation

Kafka, Flink, Kubernetes, and complex model training are intentionally outside
the first version of this project.
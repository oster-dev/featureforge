# Contributing to FeatureForge

Thank you for contributing to FeatureForge.

The project focuses on reliable Data Infrastructure, Feature Infrastructure,
and ML Platform Engineering.

## Development Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Local Infrastructure

Start Redis:

```bash
make docker-up
```

Stop Redis:

```bash
make docker-down
```

Check the Redis service:

```bash
docker exec featureforge-redis redis-cli ping
```

## Validation Before a Commit

Run:

```bash
make validate
make lint
make test
make docker-config
```

All checks should pass before opening a pull request.

## Generate Local Test Data

Use the CLI to generate reproducible local Parquet datasets:

```bash
featureforge generate \
  --config configs/synthetic_data.yaml \
  --output data/generated
```

Generated artifacts are intentionally ignored by Git. Do not commit Parquet
output, virtual environments, credentials, or local environment files.

## Development Principles

- Keep changes small and focused.
- Prefer explicit, readable code over clever abstractions.
- Preserve event-time correctness.
- Keep transformations deterministic and idempotent.
- Add tests for new behavior.
- Update documentation when architecture or behavior changes.
- Never commit credentials or generated data artifacts.
- Keep feature contracts versioned and reviewable.

## Commit Messages

Use short, imperative commit messages:

```text
feat: add deterministic behavioral event generator
fix: prevent duplicate event aggregation
test: add point-in-time leakage check
docs: document Redis online store decision
chore: update development dependencies
```

## Pull Requests

A pull request should explain:

1. What changed.
2. Why the change was needed.
3. How it was tested.
4. Whether an architecture decision changed.
5. Whether documentation was updated.

## Data and Privacy

The repository must use synthetic or publicly distributable data only.
Never commit credentials, private customer data, or local environment files.
# ADR-004: GitHub Actions CI for Reproducible Validation

**Status:** Accepted  
**Date:** 2026-09-25  
**Author:** Nico Ostermann  
**Deciders:** Nico Ostermann  

---

## Context

FeatureForge grew from a local development project into a production-grade feature platform prototype. All validation so far happened in a long-lived local `.venv` environment with:

- Pre-installed dependencies
- Cached pytest state
- Existing canonical offline feature store
- Local Redis instance for online serving tests

This created a risk: the repository might not work in a clean environment. A fresh contributor, a new CI runner, or a future self cloning the repo could encounter:

- Package import failures due to incorrect `src/` discovery
- Test failures caused by missing canonical offline data
- Silent dependency on local state that is not documented

We need reproducible, automated validation that does not depend on any local state.

---

## Decision

We implement a GitHub Actions CI workflow that validates every push to `main` and every pull request in a clean Ubuntu runner with:

- **Fresh Python 3.13 installation** — no cached dependencies, no pre-existing state
- **Editable package installation** — validates `pyproject.toml` packaging configuration
- **Package import check** — ensures `import featureforge` succeeds from a clean install
- **Ruff format check** — enforces consistent code style
- **Ruff lint** — catches type errors, unused imports, and common bugs
- **Full pytest suite** — validates all unit, integration, and simulation tests

The workflow explicitly does **not** set up:

- Redis service
- Feast feature store
- Materialized offline snapshots
- Docker Compose infrastructure

Five online-serving integration tests are skipped in CI with a clear message:

```text
Requires local Redis plus materialized FeatureForge offline snapshots.
Run Docker Compose, feast apply, and feast materialize first.
```

This establishes a **baseline CI** that validates the core platform contracts without infrastructure overhead.

---

## Consequences

### Positive

- **Reproducible validation** — every run starts from a clean state, eliminating "works on my machine" failures
- **Early packaging detection** — import failures in CI catch `src/` discovery issues before release
- **Automated quality gate** — formatting, linting, and tests run on every change
- **Transparent skips** — environment-dependent tests are explicitly skipped with documented prerequisites
- **Low maintenance** — no Redis, no Feast, no Docker Compose in CI means faster runs and fewer flakes
- **Future extensibility** — additional jobs (integration, deployment) can be added later without breaking baseline

### Negative

- **No online-serving validation in CI** — Redis-dependent tests are not executed automatically
- **Manual local validation required** — developers must run Docker Compose locally to verify online serving
- **Potential for drift** — local integration environment may diverge from what CI validates

### Mitigations

- Document local integration prerequisites in `CONTRIBUTING.md`
- Plan a future CI integration job with Redis + Feast for end-to-end serving validation
- Keep baseline CI fast and stable to encourage frequent runs

---

## Compliance

This decision aligns with the L5 roadmap goal:

> **CI/CD for automated testing and deployment**

It establishes the foundation for production-grade infrastructure practices:

- Automated validation on every change
- Clean, reproducible environments
- Explicit documentation of environment prerequisites
- Separation of baseline validation from infrastructure-heavy integration tests

---

## Notes

- Workflow file: `.github/workflows/ci.yml`
- Python version: 3.13 (latest stable as of 2026)
- Test result baseline: 141 passed, 5 skipped, 0 failed
- Skipped tests: `tests/integration/test_online_serving.py` (5 tests)

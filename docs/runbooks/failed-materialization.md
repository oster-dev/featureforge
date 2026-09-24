# Incident: Failed Feast Materialization

## Symptom

- `make materialize` or `featureforge materialize` exits with non-zero status
- `MaterializationBlockedError` raised
- Features not available in Redis (online store)
- Online lookup returns empty or stale feature values
- Demo or serving workflows fail

## Detection

Materialization failures are immediately visible:

```bash
make materialize
```

Expected error output:

```text
✗ Feast materialization blocked by quality gate
Failed checks: user_engagement_features.freshness, content_popularity_features.negative_values
Blocked manifest: output/manifests/materialize-blocked-2026-03-25T120000+0000.json
```

Or:

```text
FileNotFoundError: [Errno 2] No such file or directory: 'feature_repo'
```

## Likely causes

1. **Quality gate failure:** Invalid feature values detected (negatives, missing columns)
2. **Freshness gate failure:** Offline partitions too old for serving workflow
3. **Missing Feast repository:** `feature_repo` directory not found
4. **Feast configuration errors:** Invalid `feature_store.yaml` or source paths
5. **Redis connection failure:** Online store unreachable
6. **Schema mismatch:** Feature view schema does not match offline data
7. **Timestamp issues:** Naive datetimes, timezone mismatches

## Diagnosis

1. **Inspect blocked manifest:**

```bash
cat output/manifests/materialize-blocked-*.json | jq '.'
```

2. **Check Feast repo structure:**

```bash
find feature_repo -type f -name '*.py' | head -10
cat feature_repo/feature_store.yaml
```

3. **Verify offline store exists:**

```bash
ls -la output/offline_store/*/observation_date=*/features.parquet | head -5
```

4. **Test Feast connectivity:**

```bash
cd feature_repo
feast materialize-incremental 2026-03-25T00:00:00+00:00
```

5. **Check Redis status:**

```bash
redis-cli ping
redis-cli keys "*"
```

6. **Validate feature view definitions:**

```bash
cd feature_repo
python -c "from feature_views import user_features_view; print(user_features_view)"
```

## Recovery

1. **If blocked by quality gate:**

Fix the underlying feature quality issue (see `invalid-offline-features.md`):

```bash
# Fix feature computation, then re-run backfill
make backfill
```

2. **If blocked by freshness gate:**

Run backfill to create fresh partitions (see `stale-features.md`):

```bash
make backfill
make check-freshness
```

3. **If Feast repo missing:**

Ensure you are in the correct directory:

```bash
cd /path/to/featureforge
ls -la feature_repo/
```

4. **If Redis connection fails:**

Start Redis locally or check connection:

```bash
redis-server --daemonize yes
redis-cli ping  # Should return PONG
```

5. **Re-run materialization:**

```bash
make materialize
```

Or with explicit parameters:

```bash
featureforge materialize \
  --repo feature_repo \
  --start-time 2026-03-20T00:00:00+00:00 \
  --end-time 2026-03-25T00:00:00+00:00 \
  --manifest-output output
```

## Verification

- Materialization completes with exit code 0
- Materialization manifest written successfully
- Online lookup returns expected feature values:

```bash
cd feature_repo
python online_lookup_demo.py
```

- Demo workflow succeeds:

```bash
make demo
```

## Prevention

1. Always run quality and freshness checks before materialization
2. Validate Feast configuration in CI/CD
3. Monitor Redis health and connectivity
4. Add integration tests for materialization with various failure scenarios
5. Document materialization prerequisites (offline store, Feast repo, Redis)
6. Implement automated alerts for materialization failures
7. Use incremental materialization for production workflows (reduces blast radius)
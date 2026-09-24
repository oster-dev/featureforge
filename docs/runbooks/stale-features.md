# Incident: Stale Offline Features

## Symptom

- `make check-freshness` exits with non-zero status
- `featureforge check-freshness` reports failed freshness checks
- Materialization is blocked with `MaterializationBlockedError`
- Serving workflows cannot proceed safely

## Detection

```bash
make check-freshness
```

Or:

```bash
featureforge check-freshness \
  --reference-time 2026-03-25T00:00:00+00:00 \
  --max-lag-hours 24
```

Expected output for stale features:

```text
✗ Offline feature freshness check failed
Offline store: output/offline_store
Reference time: 2026-03-25T00:00:00+00:00
Maximum lag: 24:00:00
✗ user_engagement_features.freshness: Latest partition 2026-03-20 is 120 hours old (max: 24 hours)
✗ content_popularity_features.freshness: Latest partition 2026-03-20 is 120 hours old (max: 24 hours)
Failed checks: user_engagement_features.freshness, content_popularity_features.freshness
```

## Likely causes

1. Backfill pipeline has not run in the expected time window
2. Backfill failed silently or was skipped
3. Source data is unavailable or delayed
4. Freshness SLO is misconfigured (too strict)
5. Reference time is incorrect for the intended serving workflow

## Diagnosis

1. **Check latest partitions:**

```bash
find output/offline_store -type d -name 'observation_date=*' | sort | tail -5
```

2. **Inspect backfill manifest:**

```bash
cat output/offline_store/manifests/backfill-*.json | jq '.partitions[-1]'
```

3. **Verify source data availability:**

```bash
ls -la output/data/*.parquet
```

4. **Check freshness configuration:**

```bash
grep -A 5 "FRESHNESS" Makefile
```

## Recovery

1. **Run canonical backfill:**

```bash
make backfill
```

Or with explicit parameters:

```bash
featureforge backfill \
  --input output/data \
  --output output/offline_store \
  --start-date 2026-03-20 \
  --end-date 2026-03-25 \
  --window-days 7
```

2. **Re-run quality gate:**

```bash
featureforge check-freshness \
  --reference-time 2026-03-25T00:00:00+00:00 \
  --max-lag-hours 24
```

3. **Re-run materialization:**

```bash
make materialize
```

4. **Verify online serving:**

```bash
make demo
```

## Verification

- `make check-freshness` exits with status 0
- Freshness report shows all checks passed
- Materialization completes successfully
- Online lookup returns expected feature values

## Prevention

1. Schedule regular backfill runs (e.g., daily via cron or Airflow)
2. Monitor backfill success/failure with alerts
3. Set up freshness checks in CI/CD or scheduled jobs
4. Document freshness SLOs per feature view
5. Automate recovery: if freshness fails, trigger backfill automatically
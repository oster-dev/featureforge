# Incident: Failed Backfill

## Symptom

- `featureforge backfill` command exits with non-zero status
- No new partitions written to `output/offline_store/`
- Error messages about invalid input, missing data, or computation failures
- Downstream freshness checks fail due to missing partitions

## Detection

Backfill failures are immediately visible:

```bash
featureforge backfill \
  --input output/data \
  --output output/offline_store \
  --start-date 2026-03-25 \
  --end-date 2026-03-25 \
  --window-days 7
```

Expected error output:

```text
ValueError: dataset must contain at least one user
ValueError: window_days must be positive, got 0
ValueError: start_date (2026-03-26) must not be after end_date (2026-03-25)
```

## Likely causes

1. **Empty or missing source data:** No users, content, or events to process
2. **Invalid window_days parameter:** Zero or negative window
3. **Reversed date range:** start_date > end_date
4. **Missing input directory:** Source Parquet files not found
5. **Schema mismatch:** Source data does not match expected schema
6. **Resource exhaustion:** Out of memory during feature computation (large datasets)

## Diagnosis

1. **Check source data:**

```bash
ls -la output/data/*.parquet
duckdb -c "SELECT COUNT(*) FROM 'output/data/users.parquet'"
duckdb -c "SELECT COUNT(*) FROM 'output/data/events.parquet'"
```

2. **Verify input parameters:**

```bash
# Check date range logic
start_date="2026-03-25"
end_date="2026-03-25"
window_days=7

echo "Start: $start_date, End: $end_date, Window: $window_days"
```

3. **Inspect error logs:**

```bash
# Look for Python tracebacks in terminal output
# Check for specific error messages
```

4. **Validate source schema:**

```python
import pandas as pd
from pathlib import Path

input_dir = Path("output/data")
users = pd.read_parquet(input_dir / "users.parquet")
events = pd.read_parquet(input_dir / "events.parquet")

print("Users columns:", users.columns.tolist())
print("Events columns:", events.columns.tolist())
print("Users count:", len(users))
print("Events count:", len(events))
```

## Recovery

1. **Fix source data issues:**

If source data is missing or corrupted, regenerate:

```bash
featureforge generate \
  --config config/synthetic_data.yaml \
  --output output/data
```

2. **Correct backfill parameters:**

```bash
featureforge backfill \
  --input output/data \
  --output output/offline_store \
  --start-date 2026-03-20 \
  --end-date 2026-03-25 \
  --window-days 7
```

3. **Re-run backfill:**

```bash
make backfill
```

4. **Verify output partitions:**

```bash
find output/offline_store -name 'features.parquet' | wc -l
```

5. **Run freshness check:**

```bash
make check-freshness
```

## Verification

- Backfill completes without errors
- Expected number of partitions created in `output/offline_store/`
- Backfill manifest written successfully
- Freshness check passes
- Materialization can proceed

## Prevention

1. Validate source data before backfill (row counts, schema)
2. Add parameter validation in CLI (date ranges, window_days)
3. Implement retry logic for transient failures
4. Monitor backfill runs with alerts on failure
5. Document expected input data requirements
6. Add integration tests for backfill with various failure scenarios
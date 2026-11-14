#!/usr/bin/env bash
# Run each Flink integration module against a fresh TaskManager.
# All modules run even if one fails; the script exits non-zero if any failed.
set -uo pipefail

REST_URL="${FLINK_REST_URL:-http://127.0.0.1:8081}"
mkdir -p reports

MODULES=(
  tests/integration/test_user_play_count_kafka_to_redis.py
  tests/integration/test_user_play_count_replay_idempotency.py
  tests/integration/test_streaming_validation_routing.py
  tests/integration/test_user_play_count_late_events.py
  tests/integration/test_user_skip_rate_job.py
  tests/integration/test_feature_from_spec_job.py
  tests/integration/test_skip_rate_flink_to_redis.py
  tests/integration/test_generic_job_recovery.py
  tests/integration/test_generic_job_checkpoint_recovery.py
  tests/integration/test_generic_job_dedupe_ttl.py
  tests/integration/test_skip_rate_materialization.py
  tests/integration/test_redis_skip_rate_store.py
)

wait_for_slots() {
  local slots
  for _ in $(seq 1 60); do
    slots="$(curl -fsS "$REST_URL/overview" \
      | python -c 'import sys, json; print(json.load(sys.stdin)["slots-available"])' \
      2>/dev/null || echo 0)"
    if [ "$slots" -ge 1 ]; then
      echo "TaskManager ready with $slots free slot(s)"
      return 0
    fi
    sleep 2
  done
  echo "TaskManager did not offer a free slot in time" >&2
  return 1
}

log_taskmanager_metaspace() {
  local tm_id
  tm_id="$(curl -fsS "$REST_URL/taskmanagers" \
    | python -c 'import sys, json; print(json.load(sys.stdin)["taskmanagers"][0]["id"])' \
    2>/dev/null)" || return 0
  echo "TaskManager metaspace: $(curl -fsS \
    "$REST_URL/taskmanagers/$tm_id/metrics?get=Status.JVM.Memory.Metaspace.Used,Status.JVM.Memory.Metaspace.Max" \
    2>/dev/null || echo unavailable)"
}

status=0
for module in "${MODULES[@]}"; do
  name="$(basename "$module" .py)"
  echo "::group::Fresh TaskManager for $name"
  docker compose restart taskmanager
  wait_for_slots || { echo "::endgroup::"; status=1; continue; }
  echo "::endgroup::"

  python -m pytest -v -ra -m integration "$module" \
    --durations=5 --junitxml="reports/${name}.xml" || status=1

  log_taskmanager_metaspace
done

exit "$status"
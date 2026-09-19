from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from featureforge.backfill import (
    _daterange,
    _observation_time_for_day,
    run_backfill,
)
from featureforge.models import Content, Event, SyntheticDataset, User
from featureforge.storage import write_synthetic_dataset


def _make_user(user_id: str) -> User:
    return User(
        user_id=user_id,
        signup_at=datetime(2026, 1, 1, tzinfo=UTC),
        country="DE",
        plan_tier="premium",
        acquisition_channel="organic",
    )


def _make_content(content_id: str) -> Content:
    return Content(
        content_id=content_id,
        title=f"Title for {content_id}",
        genre="drama",
        released_at=datetime(2020, 1, 1, tzinfo=UTC),
        duration_seconds=3600,
    )


def _make_event(
    event_id: str,
    user_id: str,
    event_time: datetime,
    content_id: str = "content_0001",
    watch_seconds: int = 120,
) -> Event:
    return Event(
        event_id=event_id,
        user_id=user_id,
        content_id=content_id,
        event_type="watch",
        event_time=event_time,
        ingested_at=event_time,
        session_id="session_0001",
        device_type="web",
        watch_seconds=watch_seconds,
        is_duplicate=False,
        is_late=False,
    )


def _make_dataset() -> SyntheticDataset:
    user = _make_user("user_0001")
    content = _make_content("content_0001")

    events = [
        _make_event(
            "event_0001",
            user.user_id,
            datetime(2026, 3, 9, 12, tzinfo=UTC),
        ),
        _make_event(
            "event_0002",
            user.user_id,
            datetime(2026, 3, 10, 12, tzinfo=UTC),
        ),
        _make_event(
            "event_0003",
            user.user_id,
            datetime(2026, 3, 11, 12, tzinfo=UTC),
        ),
    ]

    return SyntheticDataset(
        users=[user],
        content_items=[content],
        events=events,
        labels=[],
    )


def test_daterange_is_inclusive():
    """The start and end date must both be included."""
    assert _daterange(date(2026, 3, 10), date(2026, 3, 12)) == [
        date(2026, 3, 10),
        date(2026, 3, 11),
        date(2026, 3, 12),
    ]


def test_daterange_rejects_reversed_dates():
    """The backfill range must not run backward."""
    with pytest.raises(ValueError, match="start_date .* must not be after end_date"):
        _daterange(date(2026, 3, 12), date(2026, 3, 10))


def test_observation_time_is_utc_midnight():
    """Each partition's observation time is midnight UTC on that date."""
    assert _observation_time_for_day(date(2026, 3, 15)) == datetime(
        2026,
        3,
        15,
        tzinfo=UTC,
    )


def test_backfill_writes_user_and_content_partitions(tmp_path: Path):
    """A backfill writes one partition per feature view and observation date."""
    output_dir = tmp_path / "offline_store"

    results, manifest_path = run_backfill(
        dataset=_make_dataset(),
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 11),
        window_days=7,
        output_dir=output_dir,
    )

    assert [result.observation_date for result in results] == [
        "2026-03-10",
        "2026-03-11",
    ]
    assert manifest_path.exists()

    for observation_date in ("2026-03-10", "2026-03-11"):
        user_path = (
            output_dir
            / "user_engagement_features"
            / f"observation_date={observation_date}"
            / "features.parquet"
        )
        content_path = (
            output_dir
            / "content_popularity_features"
            / f"observation_date={observation_date}"
            / "features.parquet"
        )

        assert user_path.exists()
        assert content_path.exists()

        assert len(pd.read_parquet(user_path)) == 1
        assert len(pd.read_parquet(content_path)) == 1


def test_backfill_is_idempotent_for_feature_partitions(tmp_path: Path):
    """Identical runs overwrite canonical partitions with identical feature values."""
    output_dir = tmp_path / "offline_store"
    dataset = _make_dataset()

    run_backfill(
        dataset=dataset,
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 11),
        window_days=7,
        output_dir=output_dir,
    )

    first_user_features = pd.read_parquet(
        output_dir / "user_engagement_features" / "observation_date=2026-03-11" / "features.parquet"
    )
    first_content_features = pd.read_parquet(
        output_dir
        / "content_popularity_features"
        / "observation_date=2026-03-11"
        / "features.parquet"
    )

    run_backfill(
        dataset=dataset,
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 11),
        window_days=7,
        output_dir=output_dir,
    )

    second_user_features = pd.read_parquet(
        output_dir / "user_engagement_features" / "observation_date=2026-03-11" / "features.parquet"
    )
    second_content_features = pd.read_parquet(
        output_dir
        / "content_popularity_features"
        / "observation_date=2026-03-11"
        / "features.parquet"
    )

    pd.testing.assert_frame_equal(first_user_features, second_user_features)
    pd.testing.assert_frame_equal(first_content_features, second_content_features)


def test_backfill_rejects_non_positive_window_days(tmp_path: Path):
    """A backfill must reject zero or negative lookback windows."""
    with pytest.raises(ValueError, match="window_days must be positive"):
        run_backfill(
            dataset=_make_dataset(),
            start_date=date(2026, 3, 10),
            end_date=date(2026, 3, 10),
            window_days=0,
            output_dir=tmp_path / "offline_store",
        )


def test_backfill_manifest_contains_run_metadata(tmp_path: Path):
    """A completed backfill writes an auditable JSON manifest."""
    output_dir = tmp_path / "offline_store"

    _, manifest_path = run_backfill(
        dataset=_make_dataset(),
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 11),
        window_days=7,
        output_dir=output_dir,
    )

    with manifest_path.open(encoding="utf-8") as file:
        manifest = json.load(file)

    assert manifest["run_type"] == "feature_backfill"
    assert manifest["status"] == "completed"
    assert manifest["start_date"] == "2026-03-10"
    assert manifest["end_date"] == "2026-03-11"
    assert manifest["window_days"] == 7
    assert manifest["output_dir"] == str(output_dir)
    assert manifest["started_at"]
    assert manifest["completed_at"]
    assert manifest["engine"] == "pandas"


def test_backfill_manifest_records_every_partition(tmp_path: Path):
    """A manifest records output paths and row counts for every backfilled day."""
    output_dir = tmp_path / "offline_store"

    _, manifest_path = run_backfill(
        dataset=_make_dataset(),
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 11),
        window_days=7,
        output_dir=output_dir,
    )

    with manifest_path.open(encoding="utf-8") as file:
        manifest = json.load(file)

    assert len(manifest["partitions"]) == 2

    first_partition = manifest["partitions"][0]
    assert first_partition["observation_date"] == "2026-03-10"
    assert first_partition["user_feature_count"] == 1
    assert first_partition["content_feature_count"] == 1
    assert Path(first_partition["user_features_path"]).exists()
    assert Path(first_partition["content_features_path"]).exists()


def test_spark_backfill_writes_partitions_and_records_engine(
    tmp_path: Path,
):
    """Spark backfill writes canonical partitions and records its engine."""
    dataset = _make_dataset()
    input_dir = tmp_path / "source_data"
    output_dir = tmp_path / "spark_offline_store"

    write_synthetic_dataset(dataset, input_dir)

    results, manifest_path = run_backfill(
        dataset=dataset,
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 11),
        window_days=7,
        output_dir=output_dir,
        engine="spark",
        input_dir=input_dir,
    )

    assert len(results) == 2
    assert manifest_path.exists()

    for observation_date in ("2026-03-10", "2026-03-11"):
        user_path = (
            output_dir
            / "user_engagement_features"
            / f"observation_date={observation_date}"
            / "features.parquet"
        )
        content_path = (
            output_dir
            / "content_popularity_features"
            / f"observation_date={observation_date}"
            / "features.parquet"
        )

        assert user_path.exists()
        assert content_path.exists()

    with manifest_path.open(encoding="utf-8") as file:
        manifest = json.load(file)

    assert manifest["engine"] == "spark"


def test_spark_and_pandas_backfills_write_identical_features(
    tmp_path: Path,
):
    """Pandas and Spark backfills must persist identical feature values."""
    dataset = _make_dataset()
    input_dir = tmp_path / "source_data"
    pandas_output_dir = tmp_path / "pandas_offline_store"
    spark_output_dir = tmp_path / "spark_offline_store"

    write_synthetic_dataset(dataset, input_dir)

    run_backfill(
        dataset=dataset,
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 11),
        window_days=7,
        output_dir=pandas_output_dir,
        engine="pandas",
    )
    run_backfill(
        dataset=dataset,
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 11),
        window_days=7,
        output_dir=spark_output_dir,
        engine="spark",
        input_dir=input_dir,
    )

    for observation_date in ("2026-03-10", "2026-03-11"):
        pandas_user_features = pd.read_parquet(
            pandas_output_dir
            / "user_engagement_features"
            / f"observation_date={observation_date}"
            / "features.parquet"
        )
        spark_user_features = pd.read_parquet(
            spark_output_dir
            / "user_engagement_features"
            / f"observation_date={observation_date}"
            / "features.parquet"
        )
        pandas_content_features = pd.read_parquet(
            pandas_output_dir
            / "content_popularity_features"
            / f"observation_date={observation_date}"
            / "features.parquet"
        )
        spark_content_features = pd.read_parquet(
            spark_output_dir
            / "content_popularity_features"
            / f"observation_date={observation_date}"
            / "features.parquet"
        )

        pd.testing.assert_frame_equal(
            pandas_user_features.sort_values("user_id").reset_index(drop=True),
            spark_user_features.sort_values("user_id").reset_index(drop=True),
        )
        pd.testing.assert_frame_equal(
            pandas_content_features.sort_values("content_id").reset_index(drop=True),
            spark_content_features.sort_values("content_id").reset_index(drop=True),
        )

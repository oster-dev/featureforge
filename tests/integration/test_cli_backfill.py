from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from featureforge.cli import main
from featureforge.config import load_synthetic_data_config
from featureforge.storage import write_synthetic_dataset
from featureforge.synthetic_data import generate_synthetic_dataset


def test_backfill_command_writes_partitioned_feature_datasets(
    tmp_path: Path,
    monkeypatch,
):
    """The backfill CLI writes stable partitioned feature datasets and a manifest."""
    config_path = Path("configs/synthetic_data.yaml")
    config = load_synthetic_data_config(config_path)
    dataset = generate_synthetic_dataset(config)

    input_dir = tmp_path / "source_data"
    output_dir = tmp_path / "offline_store"
    write_synthetic_dataset(dataset, input_dir)

    monkeypatch.setattr(
        "sys.argv",
        [
            "featureforge",
            "backfill",
            "--input",
            str(input_dir),
            "--output",
            str(output_dir),
            "--start-date",
            "2026-03-10",
            "--end-date",
            "2026-03-11",
            "--window-days",
            "7",
        ],
    )

    main()

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

    manifest_path = output_dir / "manifests" / "backfill-2026-03-10-to-2026-03-11.json"
    assert manifest_path.exists()

    with open(manifest_path, encoding="utf-8") as file:
        manifest = json.load(file)

    assert manifest["run_type"] == "feature_backfill"
    assert manifest["status"] == "completed"
    assert manifest["start_date"] == date(2026, 3, 10).isoformat()
    assert manifest["end_date"] == date(2026, 3, 11).isoformat()
    assert manifest["window_days"] == 7
    assert len(manifest["partitions"]) == 2

    first_user_features = pd.read_parquet(
        output_dir / "user_engagement_features" / "observation_date=2026-03-11" / "features.parquet"
    )
    first_content_features = pd.read_parquet(
        output_dir
        / "content_popularity_features"
        / "observation_date=2026-03-11"
        / "features.parquet"
    )

    main()

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

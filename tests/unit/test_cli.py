"""Unit tests for FeatureForge command-line contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from featureforge.cli import (
    build_parser,
    run_check_freshness_command,
)
from featureforge.models import (
    FeatureQualityCheck,
    OfflineFeatureFreshnessReport,
)


def _passing_freshness_report(
    reference_time: datetime,
) -> OfflineFeatureFreshnessReport:
    """Build a passing freshness report for CLI boundary tests."""
    return OfflineFeatureFreshnessReport(
        offline_store_dir="output/offline_store",
        reference_time=reference_time,
        max_lag_seconds=int(timedelta(days=1).total_seconds()),
        checks=[
            FeatureQualityCheck(
                name="user_engagement_features.freshness",
                passed=True,
                message="Latest user partition satisfies freshness SLO.",
            ),
            FeatureQualityCheck(
                name="content_popularity_features.freshness",
                passed=True,
                message="Latest content partition satisfies freshness SLO.",
            ),
        ],
    )


def _failing_freshness_report(
    reference_time: datetime,
) -> OfflineFeatureFreshnessReport:
    """Build a failing freshness report for CLI boundary tests."""
    return OfflineFeatureFreshnessReport(
        offline_store_dir="output/offline_store",
        reference_time=reference_time,
        max_lag_seconds=int(timedelta(days=1).total_seconds()),
        checks=[
            FeatureQualityCheck(
                name="user_engagement_features.freshness",
                passed=False,
                message="Latest user partition exceeds freshness SLO.",
            ),
            FeatureQualityCheck(
                name="content_popularity_features.freshness",
                passed=False,
                message="Latest content partition exceeds freshness SLO.",
            ),
        ],
    )


def test_generate_command_accepts_config_and_output_paths() -> None:
    """Generate accepts required config and output arguments."""
    parser = build_parser()

    args = parser.parse_args(
        [
            "generate",
            "--config",
            "configs/synthetic_data.yaml",
            "--output",
            "data/generated",
        ]
    )

    assert args.command == "generate"
    assert args.config == Path("configs/synthetic_data.yaml")
    assert args.output == Path("data/generated")


def test_check_freshness_command_uses_expected_defaults() -> None:
    """Freshness CLI defaults must represent the standard local SLO."""
    parser = build_parser()

    args = parser.parse_args(["check-freshness"])

    assert args.command == "check-freshness"
    assert args.reference_time is None
    assert args.max_lag_hours == 24


def test_check_freshness_command_accepts_explicit_arguments() -> None:
    """Freshness CLI accepts reproducible reference-time and SLO overrides."""
    parser = build_parser()

    args = parser.parse_args(
        [
            "check-freshness",
            "--reference-time",
            "2026-03-25T00:00:00+00:00",
            "--max-lag-hours",
            "48",
        ]
    )

    assert args.command == "check-freshness"
    assert args.reference_time == "2026-03-25T00:00:00+00:00"
    assert args.max_lag_hours == 48


def test_check_freshness_command_succeeds_for_passing_report() -> None:
    """A passing freshness report must complete without a process failure."""
    reference_time = datetime(2026, 3, 25, tzinfo=UTC)
    report = _passing_freshness_report(reference_time)

    with patch(
        "featureforge.cli.check_offline_feature_freshness",
        return_value=report,
    ) as check_freshness:
        run_check_freshness_command(
            reference_time="2026-03-25T00:00:00+00:00",
            max_lag_hours=24,
        )

    check_freshness.assert_called_once_with(
        Path("output/offline_store"),
        reference_time=reference_time,
        max_lag=timedelta(hours=24),
    )


def test_check_freshness_command_exits_nonzero_for_failing_report() -> None:
    """A failing freshness report must stop CLI automation with exit code one."""
    reference_time = datetime(2026, 3, 27, tzinfo=UTC)
    report = _failing_freshness_report(reference_time)

    with patch(
        "featureforge.cli.check_offline_feature_freshness",
        return_value=report,
    ) as check_freshness:
        with pytest.raises(SystemExit, match="1") as exc_info:
            run_check_freshness_command(
                reference_time="2026-03-27T00:00:00+00:00",
                max_lag_hours=24,
            )

    assert exc_info.value.code == 1
    check_freshness.assert_called_once_with(
        Path("output/offline_store"),
        reference_time=reference_time,
        max_lag=timedelta(hours=24),
    )


def test_check_freshness_command_rejects_negative_max_lag_hours() -> None:
    """Freshness CLI rejects invalid negative SLO values before store access."""
    with pytest.raises(ValueError, match="max_lag_hours must not be negative"):
        run_check_freshness_command(
            reference_time="2026-03-25T00:00:00+00:00",
            max_lag_hours=-1,
        )

"""Failure simulation: failed backfill with invalid input."""

from datetime import date, timedelta
from pathlib import Path

import pytest

from featureforge.backfill import run_backfill
from featureforge.models import Content, SyntheticDataset, User


def test_backfill_with_invalid_window_raises_error(tmp_path: Path) -> None:
    """Backfill with invalid window_days should raise a clear error."""
    users = [
        User(
            user_id="u1",
            signup_at=date.today(),
            country="US",
            plan_tier="free",
            acquisition_channel="organic",
        )
    ]
    content = [
        Content(
            content_id="c1",
            title="Test",
            genre="Action",
            released_at=date.today(),
            duration_seconds=120,
        )
    ]
    events = []
    labels = []

    dataset = SyntheticDataset(users=users, content_items=content, events=events, labels=labels)

    start_date = date.today() - timedelta(days=7)
    end_date = date.today()

    with pytest.raises(ValueError, match="window_days must be positive"):
        run_backfill(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            window_days=0,  # Invalid
            output_dir=tmp_path / "output",
            engine="pandas",
        )


def test_backfill_with_reversed_dates_raises_error(tmp_path: Path) -> None:
    """Backfill with start_date > end_date should raise a clear error."""
    users = [
        User(
            user_id="u1",
            signup_at=date.today(),
            country="US",
            plan_tier="free",
            acquisition_channel="organic",
        )
    ]
    content = [
        Content(
            content_id="c1",
            title="Test",
            genre="Action",
            released_at=date.today(),
            duration_seconds=120,
        )
    ]
    events = []
    labels = []

    dataset = SyntheticDataset(users=users, content_items=content, events=events, labels=labels)

    start_date = date.today()
    end_date = date.today() - timedelta(days=7)  # Reversed

    with pytest.raises(ValueError, match="start_date.*must not be after end_date"):
        run_backfill(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            window_days=7,
            output_dir=tmp_path / "output",
            engine="pandas",
        )


def test_backfill_with_empty_events_succeeds(tmp_path: Path) -> None:
    """Backfill with empty events should succeed (produces zero-count features)."""
    users = [
        User(
            user_id="u1",
            signup_at=date.today(),
            country="US",
            plan_tier="free",
            acquisition_channel="organic",
        )
    ]
    content = [
        Content(
            content_id="c1",
            title="Test",
            genre="Action",
            released_at=date.today(),
            duration_seconds=120,
        )
    ]
    events = []
    labels = []

    dataset = SyntheticDataset(users=users, content_items=content, events=events, labels=labels)

    start_date = date.today() - timedelta(days=1)
    end_date = date.today()

    # Should NOT raise - empty events produce zero-count features
    results, manifest_path = run_backfill(
        dataset=dataset,
        start_date=start_date,
        end_date=end_date,
        window_days=7,
        output_dir=tmp_path / "output",
        engine="pandas",
    )

    assert len(results) == 2  # 2 days
    assert manifest_path.exists()

import pytest
from pydantic import ValidationError

from featureforge.models import Event, ObservationLabel


def test_accepts_valid_late_event() -> None:
    event = Event(
        event_id="event_001",
        user_id="user_001",
        content_id="content_001",
        event_type="watch",
        event_time="2026-01-01T10:00:00Z",
        ingested_at="2026-01-01T12:00:00Z",
        session_id="session_001",
        device_type="web",
        watch_seconds=120,
        is_late=True,
    )

    assert event.is_late is True
    assert event.ingested_at > event.event_time


def test_rejects_ingestion_before_event_time() -> None:
    with pytest.raises(
        ValidationError,
        match="ingested_at must be on or after event_time",
    ):
        Event(
            event_id="event_001",
            user_id="user_001",
            event_type="click",
            event_time="2026-01-01T12:00:00Z",
            ingested_at="2026-01-01T10:00:00Z",
            session_id="session_001",
            device_type="web",
            watch_seconds=0,
        )


def test_rejects_late_flag_without_late_arrival() -> None:
    with pytest.raises(
        ValidationError,
        match="late events must have ingested_at after event_time",
    ):
        Event(
            event_id="event_001",
            user_id="user_001",
            event_type="click",
            event_time="2026-01-01T12:00:00Z",
            ingested_at="2026-01-01T12:00:00Z",
            session_id="session_001",
            device_type="web",
            watch_seconds=0,
            is_late=True,
        )


def test_rejects_invalid_observation_window() -> None:
    with pytest.raises(
        ValidationError,
        match="label_window_end must be after observation_time",
    ):
        ObservationLabel(
            label_id="label_001",
            user_id="user_001",
            observation_time="2026-01-10T00:00:00Z",
            label_window_end="2026-01-10T00:00:00Z",
            is_active_next_7d=False,
        )
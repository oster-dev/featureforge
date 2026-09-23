from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class User(BaseModel):
    user_id: str = Field(min_length=1)
    signup_at: datetime
    country: str = Field(min_length=2, max_length=3)
    plan_tier: str
    acquisition_channel: str


class Content(BaseModel):
    content_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    genre: str = Field(min_length=1)
    released_at: datetime
    duration_seconds: int = Field(gt=0)


class Event(BaseModel):
    event_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    content_id: str | None = None
    event_type: str
    event_time: datetime
    ingested_at: datetime
    session_id: str = Field(min_length=1)
    device_type: str
    watch_seconds: int = Field(ge=0)
    is_duplicate: bool = False
    is_late: bool = False

    @model_validator(mode="after")
    def validate_event_times(self) -> Event:
        if self.ingested_at < self.event_time:
            raise ValueError("ingested_at must be on or after event_time")

        if self.is_late and self.ingested_at <= self.event_time:
            raise ValueError("late events must have ingested_at after event_time")

        return self


class ObservationLabel(BaseModel):
    label_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    observation_time: datetime
    label_window_end: datetime
    is_active_next_7d: bool

    @model_validator(mode="after")
    def validate_label_window(self) -> ObservationLabel:
        if self.label_window_end <= self.observation_time:
            raise ValueError("label_window_end must be after observation_time")
        return self


class SyntheticDataset(BaseModel):
    users: list[User]
    content_items: list[Content]
    events: list[Event]
    labels: list[ObservationLabel]


class DatasetQualityReport(BaseModel):
    user_count: int = Field(ge=0)
    content_count: int = Field(ge=0)
    event_count: int = Field(ge=0)
    label_count: int = Field(ge=0)

    duplicate_count: int = Field(ge=0)
    late_event_count: int = Field(ge=0)

    unknown_event_user_reference_count: int = Field(ge=0)
    unknown_event_content_reference_count: int = Field(ge=0)
    invalid_search_content_reference_count: int = Field(ge=0)
    invalid_watch_semantics_count: int = Field(ge=0)
    invalid_event_time_order_count: int = Field(ge=0)
    invalid_late_event_count: int = Field(ge=0)

    unknown_label_user_reference_count: int = Field(ge=0)
    invalid_label_window_count: int = Field(ge=0)

    expected_duplicate_count: int = Field(ge=0)
    expected_late_event_count: int = Field(ge=0)
    expected_event_count: int = Field(ge=0)
    expected_label_count: int = Field(ge=0)

    duplicate_count_matches_expected: bool
    late_event_count_matches_expected: bool
    event_count_matches_expected: bool
    label_count_matches_expected: bool

    passed: bool


class FeatureQualityCheck(BaseModel):
    """Result of one offline feature-store quality check."""

    name: str = Field(min_length=1)
    passed: bool
    message: str = Field(min_length=1)
    affected_row_count: int = Field(ge=0, default=0)


class OfflineFeatureQualityReport(BaseModel):
    """Validation report for persisted offline feature partitions."""

    offline_store_dir: str = Field(min_length=1)
    checked_partition_paths: list[str]
    checks: list[FeatureQualityCheck]

    @property
    def passed(self) -> bool:
        """Return whether every quality check passed."""
        return all(check.passed for check in self.checks)

    @property
    def failed_checks(self) -> list[str]:
        """Return stable names for all failed checks."""
        return [check.name for check in self.checks if not check.passed]

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class UserEngagementFeatures(BaseModel):
    """Point-in-time user engagement features computed over a lookback window."""

    user_id: str = Field(min_length=1)
    observation_time: datetime
    window_days: int = Field(gt=0)

    event_count: int = Field(ge=0)
    unique_content_count: int = Field(ge=0)
    total_watch_seconds: int = Field(ge=0)
    search_count: int = Field(ge=0)
    play_count: int = Field(ge=0)
    watch_count: int = Field(ge=0)
    days_since_last_activity: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_activity_counts(self) -> UserEngagementFeatures:
        """Ensure typed event counts don't exceed total event_count."""
        typed_sum = self.search_count + self.play_count + self.watch_count
        if typed_sum > self.event_count:
            raise ValueError(
                f"Typed event counts ({typed_sum}) exceed total event_count "
                f"({self.event_count}) for user {self.user_id}"
            )
        return self


class UserFeatureBatch(BaseModel):
    """A validated batch of user engagement features for a single observation time."""

    observation_time: datetime
    window_days: int = Field(gt=0)
    features: list[UserEngagementFeatures]

    @model_validator(mode="after")
    def validate_consistent_window(self) -> UserFeatureBatch:
        """Ensure every feature record matches the batch-level observation time and window."""
        for feature in self.features:
            if feature.observation_time != self.observation_time:
                raise ValueError(
                    f"Feature for user {feature.user_id} has mismatched "
                    f"observation_time: {feature.observation_time} != {self.observation_time}"
                )
            if feature.window_days != self.window_days:
                raise ValueError(
                    f"Feature for user {feature.user_id} has mismatched "
                    f"window_days: {feature.window_days} != {self.window_days}"
                )
        return self


class ContentPopularityFeatures(BaseModel):
    """Point-in-time content popularity features computed over a lookback window."""

    content_id: str = Field(min_length=1)
    observation_time: datetime
    window_days: int = Field(gt=0)

    view_count: int = Field(ge=0)
    unique_viewer_count: int = Field(ge=0)
    total_watch_seconds: int = Field(ge=0)
    average_watch_seconds: float = Field(ge=0)
    search_count: int = Field(ge=0)
    play_count: int = Field(ge=0)
    watch_count: int = Field(ge=0)
    days_since_last_view: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_activity_counts(self) -> ContentPopularityFeatures:
        """Ensure typed event counts don't exceed total view_count."""
        typed_sum = self.search_count + self.play_count + self.watch_count
        if typed_sum > self.view_count:
            raise ValueError(
                f"Typed event counts ({typed_sum}) exceed total view_count "
                f"({self.view_count}) for content {self.content_id}"
            )
        return self


class ContentFeatureBatch(BaseModel):
    """A validated batch of content popularity features for a single observation time."""

    observation_time: datetime
    window_days: int = Field(gt=0)
    features: list[ContentPopularityFeatures]

    @model_validator(mode="after")
    def validate_consistent_window(self) -> ContentFeatureBatch:
        """Ensure every feature record matches the batch-level observation time and window."""
        for feature in self.features:
            if feature.observation_time != self.observation_time:
                raise ValueError(
                    f"Feature for content {feature.content_id} has mismatched "
                    f"observation_time: {feature.observation_time} != {self.observation_time}"
                )
            if feature.window_days != self.window_days:
                raise ValueError(
                    f"Feature for content {feature.content_id} has mismatched "
                    f"window_days: {feature.window_days} != {self.window_days}"
                )
        return self

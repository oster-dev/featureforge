"""Redis storage for append-only user play counts with guarded writes.

A thin wrapper over RedisSpecStore (ADR-009, decision 10). The constructor, the
methods and the typed model are unchanged. The ordering, the WATCH/MULTI retries
and the online.ttl expiry come from the generic store and the spec.
"""

from datetime import UTC, timedelta
from functools import cache
from pathlib import Path
from typing import Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec
from featureforge.streaming.contracts.count_vector import WriteOutcome
from featureforge.streaming.contracts.feature_record import FeatureRecord, record_fields
from featureforge.streaming.spec_store import RedisSpecStore

WriteResult = WriteOutcome

SPEC_PATH = (
    Path(__file__).resolve().parents[3] / "feature_repo" / "specs" / "user_play_count_1h.yaml"
)


@cache
def play_count_spec() -> FeatureSpec:
    return load_spec(SPEC_PATH)


class UserPlayCountFeatureV1(BaseModel):
    """A user play count for one explicit observation window."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    feature_schema_version: Literal["v1"] = "v1"
    feature_name: Literal["user_play_count_1h"] = "user_play_count_1h"
    user_id: str = Field(min_length=1)
    play_count: int = Field(ge=0, strict=True)
    observation_time: AwareDatetime
    window_start_exclusive: AwareDatetime

    @field_validator("user_id")
    @classmethod
    def reject_blank_user_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("user_id must not be blank")
        return value

    @field_validator("observation_time", "window_start_exclusive")
    @classmethod
    def normalize_timestamp(cls, value: AwareDatetime) -> AwareDatetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_window(self) -> "UserPlayCountFeatureV1":
        if self.observation_time - self.window_start_exclusive != timedelta(hours=1):
            raise ValueError("feature window must be exactly one hour")
        return self


class RedisUserPlayCountStore:
    """Maintain the latest window and its largest observed play count.

    This policy is specific to append-only counts without retractions:
    newer windows replace older ones, while counts for the same window
    can only increase. Identical deliveries do not change the value.

    This is not a general revision policy for arbitrary features and
    cannot repair an already inflated count.

    The caller owns the Redis client's connection lifecycle.
    """

    def __init__(self, client: Redis, *, key_prefix: str | None = None) -> None:
        spec = play_count_spec()
        self._store = RedisSpecStore(client, spec, key_prefix=key_prefix)
        self._name = spec.name
        self._entity_key, self._counters, self._derived = record_fields(spec)

    def key_for(self, user_id: str) -> str:
        """Return the Redis key used for this user."""
        return self._store.key_for(user_id)

    def write(self, feature: UserPlayCountFeatureV1) -> WriteResult:
        """Apply window ordering and monotone same-window corrections.

        Retry at most 16 times when concurrent writers change the key.
        Connection errors and exhausted retries propagate to the caller.
        """
        if not isinstance(feature, UserPlayCountFeatureV1):
            raise TypeError("feature must be a UserPlayCountFeatureV1 instance")
        return self._store.write(self._to_record(feature))

    def read(self, user_id: str) -> UserPlayCountFeatureV1 | None:
        """Read a validated feature, or return None when no key exists."""
        record = self._store.read(user_id)
        return None if record is None else self._to_feature(record)

    def _to_record(self, feature: UserPlayCountFeatureV1) -> FeatureRecord:
        return FeatureRecord(
            feature_name=self._name,
            entity_key=self._entity_key,
            entity_value=getattr(feature, self._entity_key),
            observation_time=feature.observation_time,
            window_start_exclusive=feature.window_start_exclusive,
            counts={name: getattr(feature, name) for name in self._counters},
            derived={name: getattr(feature, name) for name in self._derived},
        )

    def _to_feature(self, record: FeatureRecord) -> UserPlayCountFeatureV1:
        return UserPlayCountFeatureV1.model_validate(
            {
                self._entity_key: record.entity_value,
                "observation_time": record.observation_time,
                "window_start_exclusive": record.window_start_exclusive,
                **record.counts,
                **record.derived,
            }
        )

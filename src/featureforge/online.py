"""Unified, non-atomic online reads for batch and streaming features."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

Availability = Literal["present", "missing", "error"]
Freshness = Literal["fresh", "stale", "unknown", "future"]
FeatureValue = int | float | None

BATCH_FEATURES = {
    "user_engagement": (
        "user_engagement_service",
        "user_id",
        (
            "event_count",
            "unique_content_count",
            "total_watch_seconds",
            "search_count",
            "play_count",
            "watch_count",
            "days_since_last_activity",
        ),
    ),
    "content_popularity": (
        "content_popularity_service",
        "content_id",
        (
            "view_count",
            "unique_viewer_count",
            "total_watch_seconds",
            "average_watch_seconds",
            "search_count",
            "play_count",
            "watch_count",
            "days_since_last_view",
        ),
    ),
}

STREAM_FEATURE = "user_play_count_1h"

FeastReader = Callable[[str, Mapping[str, str]], Mapping[str, Any]]
StreamReader = Callable[[str], Any]


@dataclass(frozen=True)
class OnlineFeature:
    """One feature value with independently reported availability and age."""

    name: str
    entity_id: str
    backend: str
    value: FeatureValue
    availability: Availability
    observation_time: datetime | None = None
    age_seconds: float | None = None
    freshness: Freshness = "unknown"
    schema_version: str | None = None
    error_type: str | None = None


@dataclass(frozen=True)
class OnlineFeatureResponse:
    """A combined read, not an atomic cross-backend snapshot."""

    reference_time: datetime
    features: tuple[OnlineFeature, ...]
    atomic: bool = False

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["reference_time"] = self.reference_time.isoformat()
        result["features"] = [
            {
                **asdict(feature),
                "observation_time": (
                    feature.observation_time.isoformat()
                    if feature.observation_time is not None
                    else None
                ),
            }
            for feature in self.features
        ]
        return result


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must include an explicit timezone.")
    return value.astimezone(UTC)


def _identifier(value: str, name: str) -> None:
    if not value or value != value.strip() or any(char.isspace() for char in value):
        raise ValueError(f"{name} must be non-empty and contain no whitespace.")


def _first(response: Mapping[str, Any], name: str) -> Any:
    values = response.get(name)
    if values is None:
        return None
    if not isinstance(values, (list, tuple)) or len(values) != 1:
        raise ValueError(f"Expected exactly one response value for {name}.")
    return values[0]


def _numeric(value: Any) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Expected a numeric feature value.")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Feature value must be finite.")
    return value


def _feature_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Expected an epoch-seconds feature timestamp.")
    return datetime.fromtimestamp(value, tz=UTC)


def _present_feature(
    *,
    name: str,
    entity_id: str,
    backend: str,
    value: Any,
    observation_time: datetime | None,
    reference_time: datetime,
    max_age: timedelta,
    schema_version: str | None = None,
) -> OnlineFeature:
    numeric_value = _numeric(value)
    age = None
    freshness: Freshness = "unknown"

    if observation_time is not None:
        observation_time = _utc(observation_time)
        age = (reference_time - observation_time).total_seconds()
        if age < 0:
            freshness = "future"
        elif age > max_age.total_seconds():
            freshness = "stale"
        else:
            freshness = "fresh"

    return OnlineFeature(
        name=name,
        entity_id=entity_id,
        backend=backend,
        value=numeric_value,
        availability="present",
        observation_time=observation_time,
        age_seconds=age,
        freshness=freshness,
        schema_version=schema_version,
    )


class OnlineFeatureClient:
    """Read existing backends without changing their publication policies.

    Reader exceptions become explicit error records at the backend boundary.
    Raw exception messages are not included in consumer responses.
    """

    def __init__(
        self,
        *,
        feast_reader: FeastReader,
        stream_reader: StreamReader,
    ) -> None:
        self._feast_reader = feast_reader
        self._stream_reader = stream_reader

    @classmethod
    def from_local(
        cls,
        *,
        repo_path: Path,
        redis_url: str,
        stream_key_prefix: str = "featureforge-stream:online:v1:user_play_count_1h",
    ) -> OnlineFeatureClient:
        """Build lazy adapters; each read owns its backend connection lifetime."""

        def read_feast(
            service_name: str,
            entity_row: Mapping[str, str],
        ) -> Mapping[str, Any]:
            from feast import FeatureStore

            store = FeatureStore(repo_path=str(repo_path))
            return store.get_online_features(
                features=store.get_feature_service(service_name),
                entity_rows=[dict(entity_row)],
            ).to_dict(include_event_timestamps=True)

        def read_stream(user_id: str) -> Any:
            from redis import Redis

            from featureforge.streaming.online_store import RedisUserPlayCountStore

            with Redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
            ) as redis_client:
                store = RedisUserPlayCountStore(
                    redis_client,
                    key_prefix=stream_key_prefix,
                )
                return store.read(user_id)

        return cls(feast_reader=read_feast, stream_reader=read_stream)

    def _read_batch(
        self,
        group: str,
        entity_id: str,
        reference_time: datetime,
        max_age: timedelta,
    ) -> list[OnlineFeature]:
        service, entity_key, names = BATCH_FEATURES[group]
        try:
            response = self._feast_reader(service, {entity_key: entity_id})
        except Exception as error:
            return [
                OnlineFeature(
                    name=f"{group}.{name}",
                    entity_id=entity_id,
                    backend="feast",
                    value=None,
                    availability="error",
                    error_type=type(error).__name__,
                )
                for name in names
            ]

        results = []
        for name in names:
            qualified_name = f"{group}.{name}"
            try:
                value = _first(response, name)
                if value is None:
                    results.append(
                        OnlineFeature(
                            name=qualified_name,
                            entity_id=entity_id,
                            backend="feast",
                            value=None,
                            availability="missing",
                        )
                    )
                    continue

                results.append(
                    _present_feature(
                        name=qualified_name,
                        entity_id=entity_id,
                        backend="feast",
                        value=value,
                        observation_time=_feature_timestamp(_first(response, f"{name}__ts")),
                        reference_time=reference_time,
                        max_age=max_age,
                    )
                )
            except (ValueError, TypeError, OverflowError, OSError) as error:
                results.append(
                    OnlineFeature(
                        name=qualified_name,
                        entity_id=entity_id,
                        backend="feast",
                        value=None,
                        availability="error",
                        error_type=type(error).__name__,
                    )
                )
        return results

    def _read_stream(
        self,
        user_id: str,
        reference_time: datetime,
        max_age: timedelta,
    ) -> OnlineFeature:
        try:
            feature = self._stream_reader(user_id)
            if feature is None:
                return OnlineFeature(
                    name=STREAM_FEATURE,
                    entity_id=user_id,
                    backend="streaming_redis",
                    value=None,
                    availability="missing",
                )

            if feature.user_id != user_id:
                raise ValueError("Streaming feature entity does not match request.")

            return _present_feature(
                name=STREAM_FEATURE,
                entity_id=user_id,
                backend="streaming_redis",
                value=feature.play_count,
                observation_time=feature.observation_time,
                reference_time=reference_time,
                max_age=max_age,
                schema_version=feature.feature_schema_version,
            )
        except Exception as error:
            return OnlineFeature(
                name=STREAM_FEATURE,
                entity_id=user_id,
                backend="streaming_redis",
                value=None,
                availability="error",
                error_type=type(error).__name__,
            )

    def get_online_features(
        self,
        *,
        reference_time: datetime,
        batch_max_age: timedelta,
        stream_max_age: timedelta,
        user_id: str | None = None,
        content_id: str | None = None,
    ) -> OnlineFeatureResponse:
        """Read selected entity groups without fabricating missing values."""
        reference_time = _utc(reference_time)
        if batch_max_age < timedelta(0) or stream_max_age < timedelta(0):
            raise ValueError("Maximum feature ages must not be negative.")
        if user_id is None and content_id is None:
            raise ValueError("At least one entity ID is required.")
        if user_id is not None:
            _identifier(user_id, "user_id")
        if content_id is not None:
            _identifier(content_id, "content_id")

        features = []
        if user_id is not None:
            features.extend(
                self._read_batch("user_engagement", user_id, reference_time, batch_max_age)
            )
            features.append(self._read_stream(user_id, reference_time, stream_max_age))
        if content_id is not None:
            features.extend(
                self._read_batch("content_popularity", content_id, reference_time, batch_max_age)
            )

        return OnlineFeatureResponse(
            reference_time=reference_time,
            features=tuple(features),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-id")
    parser.add_argument("--content-id")
    parser.add_argument("--repo", type=Path, default=Path("feature_repo"))
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument(
        "--stream-key-prefix",
        default="featureforge-stream:online:v1:user_play_count_1h",
    )
    parser.add_argument("--reference-time", type=datetime.fromisoformat)
    parser.add_argument("--batch-max-age-seconds", type=int, required=True)
    parser.add_argument("--stream-max-age-seconds", type=int, required=True)
    args = parser.parse_args()

    client = OnlineFeatureClient.from_local(
        repo_path=args.repo,
        redis_url=args.redis_url,
        stream_key_prefix=args.stream_key_prefix,
    )
    try:
        response = client.get_online_features(
            user_id=args.user_id,
            content_id=args.content_id,
            reference_time=(
                args.reference_time if args.reference_time is not None else datetime.now(UTC)
            ),
            batch_max_age=timedelta(seconds=args.batch_max_age_seconds),
            stream_max_age=timedelta(seconds=args.stream_max_age_seconds),
        )
    except ValueError as error:
        parser.error(str(error))

    print(json.dumps(response.to_dict(), indent=2, allow_nan=False))
    return int(any(feature.availability == "error" for feature in response.features))


if __name__ == "__main__":
    raise SystemExit(main())

"""The asynchronous read path returns what the synchronous one returns (ADR-016)."""

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from featureforge.feature_service import FeatureRef, FeatureServer
from featureforge.redis_serving_reader import RedisServingReader
from featureforge.service_api import create_app
from featureforge.serving_reader import ProtocolError
from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import stamp
from featureforge.streaming.store_common import RecordRead

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
REFERENCE = datetime(2026, 11, 20, 12, tzinfo=UTC)
FRESH = REFERENCE - PLAY.freshness_slo / 2
OLD = REFERENCE - PLAY.freshness_slo * 3
ENTITIES = ["user_000001", "user_000002", "user_000003"]


def payload(observation: datetime, count: int) -> str:
    counts = {"play_count": count}
    data: dict[str, Any] = {
        "feature_schema_version": "v1",
        "feature_name": PLAY.name,
        PLAY.keys[0]: ENTITIES[0],
        "observation_time": stamp(observation),
        "window_start_exclusive": stamp(observation - timedelta(hours=1)),
        **counts,
    }
    for name, expression in PLAY.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return json.dumps(data)


def key(entity: str) -> str:
    return f"ns:v{PLAY.version}:{PLAY.name}:{entity}"


DATA = {key(ENTITIES[0]): payload(FRESH, 3), key(ENTITIES[1]): payload(OLD, 5)}


def clients(data: dict[str, str], error: Exception | None = None) -> tuple[Mock, AsyncMock]:
    sync_client, async_client = Mock(), AsyncMock()
    for client in (sync_client, async_client):
        if error is not None:
            client.mget.side_effect = error
        else:
            client.mget.side_effect = lambda keys: [data.get(k) for k in keys]
    return sync_client, async_client


def server_with(sync_client: Mock, async_client: AsyncMock | None) -> FeatureServer:
    reader = RedisServingReader(sync_client, [PLAY], namespace="ns", async_client=async_client)
    return FeatureServer(reader, [PLAY])


def outcome(call: Callable[[], Any]) -> tuple[Any, ...]:
    try:
        result = call()
    except Exception as error:
        return ("raised", type(error).__name__, str(error))
    return ("returned", result.reference_time, result.entries)


PLAN = [(PLAY, entity) for entity in ENTITIES]


@pytest.mark.parametrize(
    "answer",
    [
        [payload(FRESH, 3), None, payload(FRESH, 0)],
        [None, None, None],
        [payload(OLD, 7), payload(FRESH, 1), None],
    ],
)
def test_the_async_reader_returns_what_the_sync_reader_returns(answer: list[Any]) -> None:
    sync_client, async_client = Mock(), AsyncMock()
    sync_client.mget.return_value = answer
    async_client.mget.return_value = answer
    reader = RedisServingReader(sync_client, [PLAY], namespace="ns", async_client=async_client)

    assert asyncio.run(reader.fetch_async(PLAN)) == reader.fetch(PLAN)
    async_client.mget.assert_awaited_once_with([key(e) for e in ENTITIES])
    sync_client.mget.assert_called_once_with([key(e) for e in ENTITIES])


def test_the_async_reader_keeps_the_order_and_marks_a_missing_value() -> None:
    async_client = AsyncMock()
    async_client.mget.return_value = [payload(FRESH, 3), None]
    reader = RedisServingReader(Mock(), [PLAY], namespace="ns", async_client=async_client)

    reads = asyncio.run(reader.fetch_async(PLAN[:2]))

    assert reads[0].record is not None
    assert reads[0].record.counts["play_count"] == 3
    assert reads[1] == RecordRead()


def test_the_async_reader_rejects_the_wrong_number_of_values() -> None:
    async_client = AsyncMock()
    async_client.mget.return_value = [payload(FRESH, 3)]
    reader = RedisServingReader(Mock(), [PLAY], namespace="ns", async_client=async_client)

    with pytest.raises(ProtocolError):
        asyncio.run(reader.fetch_async(PLAN[:2]))


def test_a_failure_of_the_backend_propagates_out_of_the_async_reader() -> None:
    async_client = AsyncMock()
    async_client.mget.side_effect = ConnectionError("down")
    reader = RedisServingReader(Mock(), [PLAY], namespace="ns", async_client=async_client)

    with pytest.raises(ConnectionError):
        asyncio.run(reader.fetch_async(PLAN))


def test_a_reader_without_an_async_client_says_so() -> None:
    reader = RedisServingReader(Mock(), [PLAY], namespace="ns")

    assert reader.has_async is False
    with pytest.raises(RuntimeError):
        asyncio.run(reader.fetch_async(PLAN))


def test_the_server_uses_the_async_path_only_when_the_reader_offers_it() -> None:
    sync_client, async_client = clients(DATA)

    assert server_with(sync_client, async_client).supports_async is True
    assert server_with(sync_client, None).supports_async is False
    assert FeatureServer(Mock(), [PLAY]).supports_async is False


def test_lookup_async_returns_the_same_entries_as_lookup() -> None:
    sync_client, async_client = clients(DATA)
    server = server_with(sync_client, async_client)
    refs = [FeatureRef(PLAY.name)]
    entities = [{PLAY.keys[0]: entity} for entity in ENTITIES]

    expected = server.lookup(refs, entities, reference_time=REFERENCE)
    result = asyncio.run(server.lookup_async(refs, entities, reference_time=REFERENCE))

    assert result.reference_time == expected.reference_time
    assert result.entries == expected.entries
    assert len(result.entries) == len(ENTITIES)
    assert result.entries[0] != result.entries[2]


def test_a_failing_backend_gives_the_same_outcome_on_both_paths() -> None:
    sync_client, async_client = clients(DATA, error=ConnectionError("down"))
    server = server_with(sync_client, async_client)
    refs = [FeatureRef(PLAY.name)]
    entities = [{PLAY.keys[0]: entity} for entity in ENTITIES]

    sync_outcome = outcome(lambda: server.lookup(refs, entities, reference_time=REFERENCE))
    async_outcome = outcome(
        lambda: asyncio.run(server.lookup_async(refs, entities, reference_time=REFERENCE))
    )

    assert async_outcome == sync_outcome


def test_the_endpoint_answers_the_same_with_and_without_the_async_path() -> None:
    body = {
        "features": [{"name": PLAY.name}],
        "entities": [{PLAY.keys[0]: entity} for entity in ENTITIES],
        "reference_time": REFERENCE.isoformat(),
    }
    sync_only_client, unused = clients(DATA)
    with_async_sync_client, async_client = clients(DATA)

    threaded = TestClient(create_app(server_with(sync_only_client, None)))
    native = TestClient(create_app(server_with(with_async_sync_client, async_client)))
    threaded_answer = threaded.post("/v1/features/lookup", json=body)
    native_answer = native.post("/v1/features/lookup", json=body)

    def without_timing(answer: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in answer.items() if "elapsed" not in k}

    assert threaded_answer.status_code == native_answer.status_code == 200
    assert without_timing(native_answer.json()) == without_timing(threaded_answer.json())
    async_client.mget.assert_awaited_once()
    with_async_sync_client.mget.assert_not_called()
    sync_only_client.mget.assert_called_once()
    unused.mget.assert_not_called()

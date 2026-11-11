"""CLI output and exit-code contracts for unified online reads."""

import json
import sys
from datetime import UTC, datetime

import pytest

from featureforge.online import (
    OnlineFeature,
    OnlineFeatureClient,
    OnlineFeatureResponse,
    main,
)

REFERENCE = datetime(2026, 3, 25, tzinfo=UTC)


class FakeClient:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.arguments = None

    def get_online_features(self, **kwargs):
        self.arguments = kwargs
        if self.error is not None:
            raise self.error
        return self.response


def install_client(monkeypatch, client):
    monkeypatch.setattr(
        OnlineFeatureClient,
        "from_local",
        classmethod(lambda cls, **kwargs: client),
    )


def set_arguments(monkeypatch, *extra):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "featureforge-online",
            "--user-id",
            "user_000290",
            "--reference-time",
            REFERENCE.isoformat(),
            "--batch-max-age-seconds",
            "86400",
            "--stream-max-age-seconds",
            "1800",
            *extra,
        ],
    )


@pytest.mark.parametrize(
    ("availability", "expected_exit"),
    [
        ("present", 0),
        ("missing", 0),
        ("error", 1),
    ],
)
def test_cli_json_and_availability_exit_codes(
    monkeypatch,
    capsys,
    availability,
    expected_exit,
):
    feature = OnlineFeature(
        name="user_play_count_1h",
        entity_id="user_000290",
        backend="streaming_redis",
        value=0 if availability == "present" else None,
        availability=availability,
        error_type="ConnectionError" if availability == "error" else None,
    )
    client = FakeClient(
        response=OnlineFeatureResponse(
            reference_time=REFERENCE,
            features=(feature,),
        )
    )
    install_client(monkeypatch, client)
    set_arguments(monkeypatch)

    assert main() == expected_exit

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert captured.err == ""
    assert payload["atomic"] is False
    assert payload["features"][0]["availability"] == availability
    assert payload["features"][0]["value"] == feature.value
    assert client.arguments["reference_time"] == REFERENCE
    assert client.arguments["batch_max_age"].total_seconds() == 86400
    assert client.arguments["stream_max_age"].total_seconds() == 1800


def test_cli_validation_failure_has_exit_two_and_no_json(
    monkeypatch,
    capsys,
):
    install_client(
        monkeypatch,
        FakeClient(error=ValueError("Invalid request")),
    )
    set_arguments(monkeypatch)

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Invalid request" in captured.err


def test_cli_requires_explicit_age_limits(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["featureforge-online", "--user-id", "user_000290"],
    )

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2
    assert capsys.readouterr().out == ""

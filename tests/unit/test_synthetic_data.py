from featureforge.config import SyntheticDataConfig
from featureforge.synthetic_data import (
    generate_base_events,
    generate_content,
    generate_users,
)


def build_test_config() -> SyntheticDataConfig:
    return SyntheticDataConfig(
        seed=42,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-31T23:59:59Z",
        num_users=3,
        num_content_items=2,
        num_base_events=10,
        num_observations=5,
        duplicate_rate=0.1,
        late_event_rate=0.1,
        label_horizon_days=7,
        max_late_arrival_hours=24,
    )


def test_generates_expected_number_of_users() -> None:
    config = build_test_config()

    users = generate_users(config)

    assert len(users) == config.num_users
    assert users[0].user_id == "user_000001"
    assert users[-1].user_id == "user_000003"


def test_generates_deterministic_users() -> None:
    config = build_test_config()

    first_run = generate_users(config)
    second_run = generate_users(config)

    assert first_run == second_run


def test_generates_expected_number_of_content_items() -> None:
    config = build_test_config()

    content_items = generate_content(config)

    assert len(content_items) == config.num_content_items
    assert content_items[0].content_id == "content_000001"
    assert content_items[-1].content_id == "content_000002"


def test_generates_deterministic_content_items() -> None:
    config = build_test_config()

    first_run = generate_content(config)
    second_run = generate_content(config)

    assert first_run == second_run


def test_generates_expected_number_of_base_events() -> None:
    config = build_test_config()
    users = generate_users(config)
    content_items = generate_content(config)

    events = generate_base_events(config, users, content_items)

    assert len(events) == config.num_base_events
    assert events[0].event_id == "event_00000001"
    assert events[-1].event_id == "event_00000010"


def test_generates_deterministic_base_events() -> None:
    config = build_test_config()
    users = generate_users(config)
    content_items = generate_content(config)

    first_run = generate_base_events(config, users, content_items)
    second_run = generate_base_events(config, users, content_items)

    assert first_run == second_run


def test_base_events_reference_known_entities() -> None:
    config = build_test_config()
    users = generate_users(config)
    content_items = generate_content(config)

    user_ids = {user.user_id for user in users}
    content_ids = {item.content_id for item in content_items}
    events = generate_base_events(config, users, content_items)

    assert all(event.user_id in user_ids for event in events)
    assert all(event.content_id is None or event.content_id in content_ids for event in events)


def test_base_event_time_and_watch_semantics() -> None:
    config = build_test_config()
    users = generate_users(config)
    content_items = generate_content(config)

    events = generate_base_events(config, users, content_items)

    assert all(event.ingested_at == event.event_time for event in events)

    for event in events:
        if event.event_type in {"play", "watch"}:
            assert event.watch_seconds > 0
        else:
            assert event.watch_seconds == 0

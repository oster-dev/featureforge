from featureforge.config import SyntheticDataConfig
from featureforge.synthetic_data import (
    generate_base_events,
    generate_content,
    generate_users,
    inject_duplicates,
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


def test_injects_expected_number_of_duplicates() -> None:
    config = build_test_config()
    users = generate_users(config)
    content_items = generate_content(config)
    base_events = generate_base_events(config, users, content_items)

    events = inject_duplicates(config, base_events)

    assert len(events) == 11
    assert len([event for event in events if event.is_duplicate]) == 1


def test_duplicate_injection_is_deterministic() -> None:
    config = build_test_config()
    users = generate_users(config)
    content_items = generate_content(config)
    base_events = generate_base_events(config, users, content_items)

    first_run = inject_duplicates(config, base_events)
    second_run = inject_duplicates(config, base_events)

    assert first_run == second_run


def test_duplicate_preserves_original_event_payload() -> None:
    config = build_test_config()
    users = generate_users(config)
    content_items = generate_content(config)
    base_events = generate_base_events(config, users, content_items)

    events = inject_duplicates(config, base_events)

    duplicate = next(event for event in events if event.is_duplicate)
    original_event_id = duplicate.event_id.removesuffix("_duplicate_01")
    original = next(event for event in events if event.event_id == original_event_id)

    assert duplicate.user_id == original.user_id
    assert duplicate.content_id == original.content_id
    assert duplicate.event_type == original.event_type
    assert duplicate.event_time == original.event_time
    assert duplicate.ingested_at == original.ingested_at
    assert duplicate.session_id == original.session_id
    assert duplicate.device_type == original.device_type
    assert duplicate.watch_seconds == original.watch_seconds


def test_duplicate_injection_does_not_mutate_base_events() -> None:
    config = build_test_config()
    users = generate_users(config)
    content_items = generate_content(config)
    base_events = generate_base_events(config, users, content_items)

    inject_duplicates(config, base_events)

    assert len(base_events) == config.num_base_events
    assert all(not event.is_duplicate for event in base_events)

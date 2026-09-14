from featureforge.config import SyntheticDataConfig
from featureforge.synthetic_data import generate_content, generate_users


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
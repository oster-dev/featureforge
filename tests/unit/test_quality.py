from datetime import timedelta

from featureforge.config import SyntheticDataConfig
from featureforge.models import SyntheticDataset
from featureforge.quality import validate_synthetic_dataset
from featureforge.synthetic_data import generate_synthetic_dataset


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


def build_valid_dataset() -> tuple[SyntheticDataConfig, SyntheticDataset]:
    config = build_test_config()
    return config, generate_synthetic_dataset(config)


def test_valid_synthetic_dataset_passes_quality_validation() -> None:
    config = build_test_config()
    dataset = generate_synthetic_dataset(config)

    report = validate_synthetic_dataset(dataset, config)

    assert report.passed is True
    assert report.user_count == 3
    assert report.content_count == 2
    assert report.event_count == 11
    assert report.label_count == 5
    assert report.duplicate_count == 1
    assert report.late_event_count == 1

    assert report.unknown_event_user_reference_count == 0
    assert report.unknown_event_content_reference_count == 0
    assert report.invalid_search_content_reference_count == 0
    assert report.invalid_watch_semantics_count == 0
    assert report.invalid_event_time_order_count == 0
    assert report.invalid_late_event_count == 0
    assert report.unknown_label_user_reference_count == 0
    assert report.invalid_label_window_count == 0


def test_reports_unknown_event_user_reference() -> None:
    config, dataset = build_valid_dataset()
    invalid_event = dataset.events[0].model_copy(update={"user_id": "unknown_user"})
    invalid_dataset = dataset.model_copy(update={"events": [invalid_event, *dataset.events[1:]]})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.unknown_event_user_reference_count == 1


def test_reports_unknown_content_reference_for_non_search_event() -> None:
    config, dataset = build_valid_dataset()
    event_index = next(
        index for index, event in enumerate(dataset.events) if event.event_type != "search"
    )
    invalid_event = dataset.events[event_index].model_copy(update={"content_id": "unknown_content"})
    invalid_events = list(dataset.events)
    invalid_events[event_index] = invalid_event
    invalid_dataset = dataset.model_copy(update={"events": invalid_events})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.unknown_event_content_reference_count == 1


def test_reports_content_reference_on_search_event() -> None:
    config, dataset = build_valid_dataset()
    event_index = next(
        index for index, event in enumerate(dataset.events) if event.event_type == "search"
    )
    invalid_event = dataset.events[event_index].model_copy(update={"content_id": "content_000001"})
    invalid_events = list(dataset.events)
    invalid_events[event_index] = invalid_event
    invalid_dataset = dataset.model_copy(update={"events": invalid_events})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.invalid_search_content_reference_count == 1


def test_reports_invalid_watch_semantics() -> None:
    config, dataset = build_valid_dataset()
    event_index = next(
        index for index, event in enumerate(dataset.events) if event.event_type in {"play", "watch"}
    )
    invalid_event = dataset.events[event_index].model_copy(update={"watch_seconds": 0})
    invalid_events = list(dataset.events)
    invalid_events[event_index] = invalid_event
    invalid_dataset = dataset.model_copy(update={"events": invalid_events})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.invalid_watch_semantics_count == 1


def test_reports_invalid_event_time_order() -> None:
    config, dataset = build_valid_dataset()
    original_event = dataset.events[0]
    invalid_event = original_event.model_copy(
        update={"ingested_at": original_event.event_time - timedelta(seconds=1)}
    )
    invalid_dataset = dataset.model_copy(update={"events": [invalid_event, *dataset.events[1:]]})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.invalid_event_time_order_count == 1


def test_reports_invalid_late_event_without_positive_delay() -> None:
    config, dataset = build_valid_dataset()
    event_index = next(index for index, event in enumerate(dataset.events) if not event.is_late)
    original_event = dataset.events[event_index]
    invalid_event = original_event.model_copy(
        update={
            "ingested_at": original_event.event_time,
            "is_late": True,
        }
    )
    invalid_events = list(dataset.events)
    invalid_events[event_index] = invalid_event
    invalid_dataset = dataset.model_copy(update={"events": invalid_events})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.invalid_late_event_count == 1


def test_reports_unknown_label_user_reference() -> None:
    config, dataset = build_valid_dataset()
    invalid_label = dataset.labels[0].model_copy(update={"user_id": "unknown_user"})
    invalid_dataset = dataset.model_copy(update={"labels": [invalid_label, *dataset.labels[1:]]})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.unknown_label_user_reference_count == 1


def test_reports_invalid_label_window() -> None:
    config, dataset = build_valid_dataset()
    original_label = dataset.labels[0]
    invalid_label = original_label.model_copy(
        update={"label_window_end": original_label.observation_time}
    )
    invalid_dataset = dataset.model_copy(update={"labels": [invalid_label, *dataset.labels[1:]]})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.invalid_label_window_count == 1


def test_reports_unexpected_event_counts() -> None:
    config, dataset = build_valid_dataset()
    invalid_dataset = dataset.model_copy(update={"events": dataset.events[:-1]})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.event_count_matches_expected is False


def test_reports_unexpected_label_count() -> None:
    config, dataset = build_valid_dataset()
    invalid_dataset = dataset.model_copy(update={"labels": dataset.labels[:-1]})

    report = validate_synthetic_dataset(invalid_dataset, config)

    assert report.passed is False
    assert report.label_count_matches_expected is False

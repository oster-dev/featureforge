from __future__ import annotations

from featureforge.config import SyntheticDataConfig
from featureforge.models import DatasetQualityReport, SyntheticDataset


def validate_synthetic_dataset(
    dataset: SyntheticDataset,
    config: SyntheticDataConfig,
) -> DatasetQualityReport:
    user_ids = {user.user_id for user in dataset.users}
    content_ids = {item.content_id for item in dataset.content_items}

    unknown_event_user_reference_count = sum(
        event.user_id not in user_ids for event in dataset.events
    )

    unknown_event_content_reference_count = sum(
        event.event_type != "search"
        and (event.content_id is None or event.content_id not in content_ids)
        for event in dataset.events
    )

    invalid_search_content_reference_count = sum(
        event.event_type == "search" and event.content_id is not None for event in dataset.events
    )

    invalid_watch_semantics_count = sum(
        (event.event_type in {"play", "watch"} and event.watch_seconds <= 0)
        or (event.event_type not in {"play", "watch"} and event.watch_seconds != 0)
        for event in dataset.events
    )

    invalid_event_time_order_count = sum(
        event.ingested_at < event.event_time for event in dataset.events
    )

    invalid_late_event_count = sum(
        event.is_late and event.ingested_at <= event.event_time for event in dataset.events
    )

    unknown_label_user_reference_count = sum(
        label.user_id not in user_ids for label in dataset.labels
    )

    invalid_label_window_count = sum(
        label.label_window_end <= label.observation_time for label in dataset.labels
    )

    duplicate_count = sum(event.is_duplicate for event in dataset.events)
    late_event_count = sum(event.is_late for event in dataset.events)

    expected_duplicate_count = int(config.num_base_events * config.duplicate_rate)
    expected_event_count = config.num_base_events + expected_duplicate_count
    expected_late_event_count = int(expected_event_count * config.late_event_rate)
    expected_label_count = config.num_observations

    duplicate_count_matches_expected = duplicate_count == expected_duplicate_count
    late_event_count_matches_expected = late_event_count == expected_late_event_count
    event_count_matches_expected = len(dataset.events) == expected_event_count
    label_count_matches_expected = len(dataset.labels) == expected_label_count

    passed = all(
        (
            unknown_event_user_reference_count == 0,
            unknown_event_content_reference_count == 0,
            invalid_search_content_reference_count == 0,
            invalid_watch_semantics_count == 0,
            invalid_event_time_order_count == 0,
            invalid_late_event_count == 0,
            unknown_label_user_reference_count == 0,
            invalid_label_window_count == 0,
            duplicate_count_matches_expected,
            late_event_count_matches_expected,
            event_count_matches_expected,
            label_count_matches_expected,
        )
    )

    return DatasetQualityReport(
        user_count=len(dataset.users),
        content_count=len(dataset.content_items),
        event_count=len(dataset.events),
        label_count=len(dataset.labels),
        duplicate_count=duplicate_count,
        late_event_count=late_event_count,
        unknown_event_user_reference_count=unknown_event_user_reference_count,
        unknown_event_content_reference_count=(unknown_event_content_reference_count),
        invalid_search_content_reference_count=(invalid_search_content_reference_count),
        invalid_watch_semantics_count=invalid_watch_semantics_count,
        invalid_event_time_order_count=invalid_event_time_order_count,
        invalid_late_event_count=invalid_late_event_count,
        unknown_label_user_reference_count=(unknown_label_user_reference_count),
        invalid_label_window_count=invalid_label_window_count,
        expected_duplicate_count=expected_duplicate_count,
        expected_late_event_count=expected_late_event_count,
        expected_event_count=expected_event_count,
        expected_label_count=expected_label_count,
        duplicate_count_matches_expected=duplicate_count_matches_expected,
        late_event_count_matches_expected=late_event_count_matches_expected,
        event_count_matches_expected=event_count_matches_expected,
        label_count_matches_expected=label_count_matches_expected,
        passed=passed,
    )

"""Compute a count/ratio feature from a FeatureSpec in continuous or bounded mode."""

import argparse
import json

from pyflink.common import Configuration, Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows

from featureforge.spec.flink_plan import (
    accumulate,
    build_flink_plan,
    dedupe_value,
    entity_value,
    feature_record,
    late_event_record,
)
from featureforge.spec.flink_plan import merge as merge_counts
from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.contracts.feature_time import (
    right_closed_timestamp_ms,
)
from featureforge.streaming.reference.user_play_count import (
    EventIdentityConflictError,
)

BOOTSTRAP_SERVERS = "kafka:29092"


class FeatureTimestampAssigner(TimestampAssigner):
    """Preserve the reference's right-closed window membership."""

    def extract_timestamp(self, value, record_timestamp):
        event = InteractionEventV1.model_validate_json(value)
        return right_closed_timestamp_ms(event.event_time)


class DeduplicateEvents(KeyedProcessFunction):
    """Deduplicate by the spec's dedupe key within the spec's retention horizon."""

    def __init__(self, dedupe_ttl_ms: int):
        self.dedupe_ttl_ms = dedupe_ttl_ms

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("logical-event-payload-v1", Types.STRING())
        descriptor.enable_time_to_live(
            StateTtlConfig.new_builder(Time.milliseconds(self.dedupe_ttl_ms))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        self.logical_payload = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        event = InteractionEventV1.model_validate_json(value)
        logical_payload = json.dumps(
            event.model_dump(mode="json", exclude={"ingested_at"}),
            sort_keys=True,
            separators=(",", ":"),
        )
        previous_payload = self.logical_payload.value()

        if previous_payload is None:
            self.logical_payload.update(logical_payload)
            yield value
            return

        if previous_payload != logical_payload:
            raise EventIdentityConflictError(
                f"Conflicting logical payload for event_id={event.event_id}"
            )


class CountEvents(AggregateFunction):
    """Keep one integer per counter of the plan for each entity and window."""

    def __init__(self, plan):
        self.plan = plan

    def create_accumulator(self):
        return self.plan.zero

    def add(self, value, accumulator):
        return accumulate(self.plan, json.loads(value), tuple(accumulator))

    def get_result(self, accumulator):
        return accumulator

    def merge(self, left, right):
        return merge_counts(tuple(left), tuple(right))


class EmitFeature(ProcessWindowFunction):
    """Attach entity and reference-compatible window metadata."""

    def __init__(self, plan):
        self.plan = plan

    def process(self, key, context, elements):
        window = context.window()
        accumulator = tuple(int(count) for count in next(iter(elements)))
        yield feature_record(self.plan, key, window.start, window.end, accumulator)


def kafka_sink(topic: str, *, bootstrap_servers: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute a count/ratio feature from a spec.")
    parser.add_argument("--spec", required=True)
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--input-topic", default=None)
    parser.add_argument("--feature-topic", default=None)
    parser.add_argument("--late-topic", default=None)
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--job-name", default=None)
    parser.add_argument("--bounded", action="store_true")
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="Disable automatic job restarts for fail-fast diagnostic runs.",
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    spec = load_spec(arguments.spec)
    plan = build_flink_plan(spec)
    slug = plan.feature_name.replace("_", "-")

    input_topic = arguments.input_topic or spec.source
    feature_topic = arguments.feature_topic or f"features.{slug}.v1"
    late_topic = arguments.late_topic or f"interaction-events.{slug}.late.v1"
    group_id = arguments.group_id or f"featureforge-stream-{slug}-spec-v1"
    job_name = arguments.job_name or f"featureforge-stream-{slug}-spec"
    late_events = OutputTag(f"{slug}-late", Types.STRING())

    configuration = Configuration()
    if arguments.no_restart:
        configuration.set_string("restart-strategy.type", "none")

    env = StreamExecutionEnvironment.get_execution_environment(configuration)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)

    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(arguments.bootstrap_servers)
        .set_topics(input_topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    )

    if arguments.bounded:
        source_builder.set_bounded(KafkaOffsetsInitializer.latest())

    raw_events = env.from_source(
        source_builder.build(),
        WatermarkStrategy.no_watermarks(),
        "validated-interaction-events-source",
    )

    timed_events = raw_events.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_millis(plan.watermark_delay_ms)
        ).with_timestamp_assigner(FeatureTimestampAssigner())
    )

    unique_events = (
        timed_events.key_by(lambda payload: dedupe_value(plan, payload), key_type=Types.STRING())
        .process(DeduplicateEvents(plan.dedupe_ttl_ms), output_type=Types.STRING())
        .name("deduplicate-by-dedupe-key")
        .uid(f"{slug}-spec-deduplicate-v1")
    )

    features = (
        unique_events.key_by(lambda payload: entity_value(plan, payload), key_type=Types.STRING())
        .window(
            SlidingEventTimeWindows.of(
                Time.milliseconds(plan.window_size_ms),
                Time.milliseconds(plan.window_slide_ms),
            )
        )
        .allowed_lateness(plan.allowed_lateness_ms)
        .side_output_late_data(late_events)
        .aggregate(
            CountEvents(plan),
            EmitFeature(plan),
            accumulator_type=Types.TUPLE([Types.LONG()] * len(plan.counters)),
            output_type=Types.STRING(),
        )
        .name(f"{slug}-spec-windows")
        .uid(f"{slug}-spec-windows-v1")
    )

    features.sink_to(kafka_sink(feature_topic, bootstrap_servers=arguments.bootstrap_servers)).name(
        f"{slug}-spec-kafka-sink"
    )

    features.get_side_output(late_events).map(
        lambda payload: late_event_record(plan, payload),
        output_type=Types.STRING(),
    ).sink_to(kafka_sink(late_topic, bootstrap_servers=arguments.bootstrap_servers)).name(
        "late-events-kafka-sink"
    )

    env.execute(job_name)


if __name__ == "__main__":
    main()

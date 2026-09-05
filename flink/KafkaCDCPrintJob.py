from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("sales.public.customers")
        .set_group_id("pyflink-cdc-print-test-v3")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka CDC Source",
    )

    stream.print()

    env.execute("PyFlink Kafka CDC Print Test")


if __name__ == "__main__":
    main()

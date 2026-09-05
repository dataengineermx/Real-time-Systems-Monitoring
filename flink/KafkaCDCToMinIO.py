import json

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.configuration import Configuration
from pyflink.common.serialization import ByteArraySchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.formats.parquet import ParquetBulkWriters
from pyflink.table import DataTypes


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "sales.public.customers"
KAFKA_GROUP_ID = "pyflink-cdc-bronze-v2"

OUTPUT_PATH = "s3://bronze/customers/"


# -------------------------------------------------------------------
# TypeInformation for DataStream.map()
#
# IMPORTANT:
# DataStream.map() requires PyFlink TypeInformation.
# Do NOT use DataTypes.ROW() here.
# -------------------------------------------------------------------

CDC_TYPE_INFO = Types.ROW_NAMED(
    [
        "id",
        "before_json",
        "after_json",
        "source_json",
        "op",
        "ts_ms",
        "ts_us",
        "ts_ns",
    ],
    [
        Types.LONG(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
    ],
)


# -------------------------------------------------------------------
# DataTypes schema for Parquet
#
# This is different from CDC_TYPE_INFO and is used by
# ParquetBulkWriters.
# -------------------------------------------------------------------

PARQUET_ROW_TYPE = DataTypes.ROW(
    [
        DataTypes.FIELD("id", DataTypes.BIGINT()),
        DataTypes.FIELD("before_json", DataTypes.STRING()),
        DataTypes.FIELD("after_json", DataTypes.STRING()),
        DataTypes.FIELD("source_json", DataTypes.STRING()),
        DataTypes.FIELD("op", DataTypes.STRING()),
        DataTypes.FIELD("ts_ms", DataTypes.BIGINT()),
        DataTypes.FIELD("ts_us", DataTypes.BIGINT()),
        DataTypes.FIELD("ts_ns", DataTypes.BIGINT()),
    ]
)



def parse_cdc_event(message):
    try:
        if message is None:
            print("INFO: Kafka tombstone received; ignoring.")
            return None

        if isinstance(message, bytes):
            message = message.decode("utf-8")

        event = json.loads(message)

        payload = event.get("payload", {})

        before = payload.get("before")
        after = payload.get("after")
        source = payload.get("source")

        customer_id = None

        if after is not None:
            customer_id = after.get("id")

        if customer_id is None and before is not None:
            customer_id = before.get("id")

        before_json = (
            json.dumps(
                before,
                separators=(",", ":"),
                ensure_ascii=False
            )
            if before is not None
            else None
        )

        after_json = (
            json.dumps(
                after,
                separators=(",", ":"),
                ensure_ascii=False
            )
            if after is not None
            else None
        )

        source_json = (
            json.dumps(
                source,
                separators=(",", ":"),
                ensure_ascii=False
            )
            if source is not None
            else None
        )

        return Row(
            customer_id,
            before_json,
            after_json,
            source_json,
            payload.get("op"),
            payload.get("ts_ms"),
            payload.get("ts_us"),
            payload.get("ts_ns"),
        )

    except Exception as exc:
        print(
            f"ERROR parsing CDC event: {exc}. "
            f"Message={message}"
        )
        return None




        # ------------------------------------------------------------
        # Debezium event
        # ------------------------------------------------------------

        before = event.get("before")
        after = event.get("after")
        source = event.get("source")

        # ------------------------------------------------------------
        # Customer ID
        #
        # INSERT / UPDATE / SNAPSHOT:
        #     id comes from after
        #
        # DELETE:
        #     after is null, so use before
        # ------------------------------------------------------------

        customer_id = None

        if after is not None:
            customer_id = after.get("id")

        if customer_id is None and before is not None:
            customer_id = before.get("id")

        # ------------------------------------------------------------
        # Preserve original CDC structures as JSON strings
        # ------------------------------------------------------------

        before_json = (
            json.dumps(
                before,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            if before is not None
            else None
        )

        after_json = (
            json.dumps(
                after,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            if after is not None
            else None
        )

        source_json = (
            json.dumps(
                source,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            if source is not None
            else None
        )

        # ------------------------------------------------------------
        # Return PyFlink Row
        # ------------------------------------------------------------

        return Row(
            customer_id,
            before_json,
            after_json,
            source_json,
            event.get("op"),
            event.get("ts_ms"),
            event.get("ts_us"),
            event.get("ts_ns"),
        )

    except Exception as exc:

        print(
            "ERROR parsing CDC event: "
            f"{exc}. Message={message}"
        )

        return None


def main():

    # ---------------------------------------------------------------
    # Flink environment
    # ---------------------------------------------------------------

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)

#    env.get_config().set(
#        "pipeline.jars",
#        "file:///opt/flink/lib/flink-parquet-2.3.0.jar"
#    )

    # FileSink requires checkpoints in streaming mode.
    env.enable_checkpointing(10000)

    # ---------------------------------------------------------------
    # Kafka Source
    # ---------------------------------------------------------------

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            ByteArraySchema()
        )
        .build()
    )

    kafka_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka CDC Source",
    )

    # ---------------------------------------------------------------
    # Parse CDC JSON
    #
    # IMPORTANT:
    # output_type uses Types.ROW_NAMED()
    # NOT DataTypes.ROW()
    # ---------------------------------------------------------------

    bronze_stream = (
        kafka_stream
        .map(
            parse_cdc_event,
            output_type=CDC_TYPE_INFO,
        )
        .filter(
            lambda row: row is not None
        )
    )

    # ---------------------------------------------------------------
    # Hadoop configuration
    # ---------------------------------------------------------------

    hadoop_config = Configuration()

    # ---------------------------------------------------------------
    # Parquet + MinIO Sink
    # ---------------------------------------------------------------

    sink = (
        FileSink
        .for_bulk_format(
            OUTPUT_PATH,
            ParquetBulkWriters.for_row_type(
                PARQUET_ROW_TYPE,
                hadoop_config=hadoop_config,
                utc_timestamp=True,
            ),
        )
        .build()
    )

    # ---------------------------------------------------------------
    # Connect stream to sink
    # ---------------------------------------------------------------

    bronze_stream.sink_to(sink)

    # ---------------------------------------------------------------
    # Execute
    # ---------------------------------------------------------------

    env.execute(
        "Kafka CDC To MinIO Bronze Parquet"
    )


if __name__ == "__main__":
    main()

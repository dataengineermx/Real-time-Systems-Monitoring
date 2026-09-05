import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.connector.file.sink.FileSink;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import org.apache.flink.core.fs.Path;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

public class KafkaCDCToMinIOTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpoints so FileSink can finalize files
        env.enableCheckpointing(10000);

        // ------------------------------------------------------------
        // Kafka deserializer
        // ------------------------------------------------------------

        KafkaRecordDeserializationSchema<String> deserializer =
                new KafkaRecordDeserializationSchema<String>() {

                    @Override
                    public void deserialize(
                            ConsumerRecord<byte[], byte[]> record,
                            Collector<String> out) {

                        // Ignore Kafka tombstones
                        if (record.value() == null) {
                            return;
                        }

                        String value = new String(
                                record.value(),
                                StandardCharsets.UTF_8
                        );

                        out.collect(value);
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                };

        // ------------------------------------------------------------
        // Kafka source
        // ------------------------------------------------------------

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka:29092")
                        .setTopics("sales.public.customers")
                        .setGroupId("flink-cdc-minio-test")
                        .setStartingOffsets(
                                OffsetsInitializer.earliest()
                        )
                        .setDeserializer(deserializer)
                        .build();

        DataStream<String> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka CDC Source"
                );

        // ------------------------------------------------------------
        // MinIO / S3 FileSink
        // ------------------------------------------------------------

        FileSink<String> sink =
                FileSink
                        .forRowFormat(
                                new Path("s3://bronze/test/"),
                                new SimpleStringEncoder<String>("UTF-8")
                        )
                        .build();

        stream.sinkTo(sink);

        // ------------------------------------------------------------
        // Execute
        // ------------------------------------------------------------

        env.execute("Kafka CDC To MinIO Test");
    }
}

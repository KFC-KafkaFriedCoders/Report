package org.example.payment_guard;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import java.io.IOException;

public class Main {
    // private static final String KAFKA_BOOTSTRAP_SERVERS = "13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9094";
    private static final String SOURCE_TOPIC = "test-topic";
    private static final String TARGET_TOPIC = "3_non_response";
    private static final String APP_NAME = "3_non_response_passthrough";

    // byte[] 전용 직렬화/역직렬화 스키마
    public static class ByteArraySchema implements DeserializationSchema<byte[]>, SerializationSchema<byte[]> {
        @Override
        public byte[] deserialize(byte[] message) throws IOException {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public byte[] serialize(byte[] element) {
            return element;
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<byte[]> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInformation.of(byte[].class);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setMaxParallelism(10);

        // Kafka Source: Avro 메시지를 byte[]로 수신
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(APP_NAME)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ByteArraySchema())
                .build();

        // Kafka Sink: byte[] 그대로 전송
        KafkaSink<byte[]> sink = KafkaSink.<byte[]>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TARGET_TOPIC)
                        .setValueSerializationSchema(new ByteArraySchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 데이터 흐름 연결
        DataStream<byte[]> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Raw Byte Kafka Source");
        stream.sinkTo(sink);

        env.execute("Kafka Byte Passthrough from test-topic to 3_non_response");
    }
}
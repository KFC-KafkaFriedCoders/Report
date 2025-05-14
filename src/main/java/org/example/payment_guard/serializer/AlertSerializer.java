package org.example.payment_guard.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.payment_guard.model.Alert;

public class AlertSerializer implements KafkaRecordSerializationSchema<Alert> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;

    public AlertSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Alert alert, KafkaSinkContext context, Long timestamp) {
        try {
            byte[] value = objectMapper.writeValueAsBytes(alert);
            return new ProducerRecord<>(topic, value);
        } catch (JsonProcessingException e) {
            // 에러 로깅
            System.err.println("JSON 직렬화 에러: " + e.getMessage());
            return null;
        }
    }
}

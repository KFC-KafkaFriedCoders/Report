package org.example.payment_guard.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.example.payment_guard.model.Payment;

import java.io.IOException;

/**
 * Kafka에서 수신한 JSON 메시지를 Payment 객체로 변환하는 디저라이저입니다.
 */
public class PaymentDeserializer implements DeserializationSchema<Payment> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Payment deserialize(byte[] message) throws IOException {
        try {
            return objectMapper.readValue(message, Payment.class);
        } catch (Exception e) {
            // 디버깅을 위한 로깅
            System.err.println("JSON 파싱 에러: " + new String(message) + ", 오류: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Payment nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Payment> getProducedType() {
        return TypeInformation.of(Payment.class);
    }
}

package org.example.payment_guard.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SimplePojoDeserializer<T> implements DeserializationSchema<T> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> clazz;

    public SimplePojoDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        // UTF-8 로 디코딩
        String json = new String(message, StandardCharsets.UTF_8);
        return objectMapper.readValue(json, clazz);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }
}
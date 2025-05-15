package org.example.payment_guard;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * byte[] 전용 직렬화/역직렬화 스키마
 */
public class ByteArraySchema implements DeserializationSchema<byte[]>, SerializationSchema<byte[]> {
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
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}

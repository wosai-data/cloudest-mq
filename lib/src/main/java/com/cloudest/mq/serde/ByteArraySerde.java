package com.cloudest.mq.serde;

import java.io.IOException;

public class ByteArraySerde implements  Serializer<byte[]>, Deserializer<byte[]> {

    @Override public byte[] serialize(byte[] message) throws IOException {
        return message;
    }

    @Override public byte[] deserialize(byte[] message) throws IOException {
        return message;
    }
}

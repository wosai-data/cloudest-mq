package com.cloudest.mq.serde.json;

import com.cloudest.mq.serde.Deserializer;
import com.cloudest.mq.serde.Serializer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

public class JsonSerde<T> implements Serializer<T>, Deserializer<T> {

    private final ObjectMapper mapper;
    private final Class<T> tClass;

    public JsonSerde(Class<T> tClass) {
        this.mapper = new ObjectMapper();
        this.tClass = tClass;
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, Boolean.FALSE);
    }

    @Override public T deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0)
            return null;
        T message = mapper.readValue(bytes, tClass);
        return message;
    }

    @Override public byte[] serialize(T message) throws IOException {
        mapper.configure(SerializationFeature.INDENT_OUTPUT, Boolean.FALSE);
        return mapper.writeValueAsString(message).getBytes();
    }
}

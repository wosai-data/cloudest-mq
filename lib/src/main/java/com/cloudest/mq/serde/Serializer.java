package com.cloudest.mq.serde;

import java.io.IOException;

/**
 * The serializer describes how to turn a data object into byte array
 *
 * @param <T> The type to be serialized.
 */

public interface Serializer<T> {

    /**
     * Serializes the message to byte array
     *
     * @param message The message to be serialized
     * @return The serialized byte array
     */
    byte[] serialize(T message) throws IOException;
}


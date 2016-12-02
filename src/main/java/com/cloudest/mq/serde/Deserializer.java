package com.cloudest.mq.serde;

import java.io.IOException;

/**
 * The deserializer describes
 * how to turn the byte messages delivered by message queue(RabbitMQ or Kakfa)
 * into Java data types
 *
 * @param <T> The type created by the deserializer
 */
public interface Deserializer<T> {

    /**
     * Deserializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The deserialized message as an object.
     */
    T deserialize(byte[] message) throws IOException;
}


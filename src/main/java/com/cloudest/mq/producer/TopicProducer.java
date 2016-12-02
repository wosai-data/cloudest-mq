package com.cloudest.mq.producer;

public interface TopicProducer<K, V> {

    public void open();

    /**
     * Post the given Key/Value based record synchronously to the server.
     *
     * @param key The key of the record
     * @param value The value of the record
     */
    public void post(K key, V value);

    /**
     * Flush pending records.
     */
    public void flush();

    /**
     * Close this producer
     */
    public void close();

}


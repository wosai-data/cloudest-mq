package com.cloudest.mq.consumer;

public interface TopicConsumer<T> {
    /**
     * Open the consumer for fetching data
     */
    void open();
    /**
     * Fetch data for the topic. Blocked
     */
    T getNext() throws InterruptedException;

    /**
     * Commit the last consumed record returned on the last getNext() call
     * for all the subscribed list of topics and partitions.
     */
    void commit() throws InterruptedException;

    /**
     * Close the consumer, waiting indefinitely for any needed cleanup.
     */
    void close();
}

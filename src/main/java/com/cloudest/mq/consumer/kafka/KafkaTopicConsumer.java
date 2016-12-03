package com.cloudest.mq.consumer.kafka;

import com.cloudest.mq.consumer.TopicConsumer;
import com.cloudest.mq.serde.Deserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaTopicConsumer<T> extends Thread implements TopicConsumer<T>  {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicConsumer.class);
    private final Deserializer<T> deserializer;
    private final TopicRecordQueue recordQueue;
    private final ConsumerThread consumerThread;

    /**
     *
     * @param topic
     * @param appId The same appId will share the same kafka consumer group
     * @param deserializer deserializer for value
     * @param properties
     */

    public KafkaTopicConsumer(String topic, String appId, Deserializer<T> deserializer, Properties properties) {
        // TODO make these configurable
        int bufferSize = Integer.parseInt(properties.getProperty("cloudest.mq.consumer.kafka.record.queue.buffer.size", "1000"));
        this.recordQueue = new TopicRecordQueue(topic, bufferSize);
        this.consumerThread = new ConsumerThread(topic, appId, recordQueue, properties);
        this.deserializer = deserializer;
    }

    @Override public void open() {
        consumerThread.start();
    }

    @Override public T getNext() throws InterruptedException {
        ConsumerRecord<byte[], byte[]> record = recordQueue.poll();
        if (record == null)
            return null;
        final T value;
        try {
            value = deserializer.deserialize(record.value());
        } catch (Exception e) {
            log.error("Failed to deserialize value for record. topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), e);
            return null;
        }
        return value;
    }

    @Override public void commit() throws InterruptedException {
        consumerThread.commit();
    }

    @Override public void close() {
        consumerThread.close();
        consumerThread.interrupt();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.warn("Failed to join consumer thread", e);
        }
    }
}

package com.cloudest.mq.producer.kafka;

import com.cloudest.mq.producer.TopicProducer;
import com.cloudest.mq.serde.Serializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;


public class KafkaTopicProducer<K, V> extends Thread implements TopicProducer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicProducer.class);
    private final String topic;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    private KafkaProducer<byte[], byte[]> producer;

    private Callback postCallback;

    private long pendingRecords;
    private final Object pendingRecordsLock = new Object();

    public KafkaTopicProducer(String topic, Serializer<K> keySerializer, Serializer<V> valueSerializer, Properties properties) {
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        if (properties.contains(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            log.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        }
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

        if (properties.contains(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            log.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

        this.producer = new KafkaProducer<>(properties);
    }

    @Override public void open() {

        postCallback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    log.error("Failed to post record to Kafka: " + e.getMessage(), e);
                }
                acknowledgeMessage();
            }
        };
    }

    @Override
    public void post(K key, V value) {
        byte[] serializedKey = null, serializedValue = null;
        try {
            serializedKey = keySerializer.serialize(key);
            serializedValue = valueSerializer.serialize(value);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, serializedKey, serializedValue);
        synchronized (pendingRecordsLock) {
            pendingRecords++;
        }
        producer.send(record, postCallback);
    }


    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    private void acknowledgeMessage() {
        synchronized (pendingRecordsLock) {
            pendingRecords--;
            if (pendingRecords == 0) {
                pendingRecordsLock.notifyAll();
            }
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }
}


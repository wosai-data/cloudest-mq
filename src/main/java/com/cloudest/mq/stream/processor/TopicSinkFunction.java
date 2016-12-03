package com.cloudest.mq.stream.processor;

import com.cloudest.mq.producer.TopicProducer;
import com.cloudest.mq.serde.KeyValue;

/**
 *  Sink tuple to a topic message queue
 */
public class TopicSinkFunction<K, V> extends SinkFunction<KeyValue<K, V>>{

    private final TopicProducer<K, V> producer;

    public TopicSinkFunction(TopicProducer<K, V> producer) {
        this.producer = producer;
    }
    @Override public void sink(KeyValue<K, V> tuple) {
        producer.post(tuple.key(), tuple.value());
    }
}

package com.cloudest.mq.stream.processor;

import com.cloudest.mq.consumer.TopicConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicSourceFunction<T> extends SourceFunction<T> {

    private final TopicConsumer<T> consumer;
    private static final Logger log = LoggerFactory.getLogger(TopicSinkFunction.class);

    public TopicSourceFunction(TopicConsumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override public T nextTuple() {
        try {
            return consumer.getNext();
        } catch (InterruptedException e) {
            log.warn("Consumer got interrupted", e);
            return null;
        }
    }

    @Override public void commit() {
        try {
            consumer.commit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override public void open() {
        consumer.open();
    }

    @Override public void close() {
        consumer.close();
    }
}

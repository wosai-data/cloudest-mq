package com.cloudest.mq.stream.processor;

/**
 *
 * Processor to sink tuple to some storage.
 * For example, KafkaTopic or file or stdout
 */

public class SinkProcessor<T> extends Processor<T> {

    private final SinkFunction<T> sinkFunction;

    public SinkProcessor(String name, SinkFunction<T> sinkFunction) {
        super(name, sinkFunction);
        this.sinkFunction = sinkFunction;
    }

    @Override public void process(T tuple) {
        this.sinkFunction.sink(tuple);
    }

    @Override public void close() {

    }
}

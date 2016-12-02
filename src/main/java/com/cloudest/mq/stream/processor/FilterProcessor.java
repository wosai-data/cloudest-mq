package com.cloudest.mq.stream.processor;

public class FilterProcessor<T> extends Processor<T> {

    private final FilterFunction<T> filter;

    public FilterProcessor(String name, FilterFunction<T> filter) {
        super(name, filter);
        this.filter = filter;
    }

    @Override public void process(T tuple) {
        if (filter.accept(tuple))
            context().forward(tuple);
    }

    @Override public void close() {
    }
}

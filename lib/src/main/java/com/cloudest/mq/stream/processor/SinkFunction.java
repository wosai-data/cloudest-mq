package com.cloudest.mq.stream.processor;

public abstract class SinkFunction<T> extends Function {

    public abstract void sink(T tuple);

}

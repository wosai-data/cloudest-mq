package com.cloudest.mq.stream.processor;

public abstract class SourceFunction<T> extends Function {

    /**
     * @return tuple to be processed
     * @note null tuple won't be forward to descendent processors
     */
    public abstract T nextTuple();

    public abstract void commit();

    public abstract void open();

    public abstract void close();
}

package com.cloudest.mq.stream.processor;

public abstract class FilterFunction<T> extends Function {
    /**
     * @param tuple The tuple to be filtered.
     * @return True for values that should be retained, false for values to be filtered out.
     */
    public abstract boolean accept(T tuple);
}

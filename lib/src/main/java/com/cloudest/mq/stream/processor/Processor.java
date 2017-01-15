package com.cloudest.mq.stream.processor;

import java.util.ArrayList;
import java.util.List;

/**
 * Processor is used for defining the stream computation logic
 * This class is mainly used in DataStream and StreamTask
 */

public abstract class Processor<T> {

    private final List<Processor<?>> children;
    private final String name;
    private final Function function;
    private ProcessorContext context;

    public Processor(String name, Function function) {
        this.name = name;
        this.function = function;
        this.children = new ArrayList<>();
    }

    public final String name() {
        return name;
    }

    public void addChild(Processor<?> child) {
        children.add(child);
    }

    public final List<Processor<?>> children() {
        return children;
    }

    public void init(ProcessorContext context) {
        this.context = context;
        this.function.init(context);
    }

    public ProcessorContext context() {
        return context;
    }

    /**
     * method for processing the input tuple
     * @param tuple
     */
    public abstract void process(T tuple);

    public abstract void close();

}

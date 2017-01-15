package com.cloudest.mq.stream.processor;

public class ApplyProcessor<IN, OUT> extends Processor<IN> {

    private final ApplyFunction<IN, OUT> applyFunction;

    public ApplyProcessor(String name, ApplyFunction<IN, OUT> applyFunction) {
        super(name, applyFunction);
        this.applyFunction = applyFunction;
    }

    @Override public void process(IN tuple) {
        OUT outTuple = applyFunction.apply(tuple);
        context().forward(outTuple);
    }

    @Override public void close() {
    }
}

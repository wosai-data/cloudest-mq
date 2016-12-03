package com.cloudest.mq.stream.processor;

public abstract class Function {
    ProcessorContext context;

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public ProcessorContext context(){
        return context;
    }
}

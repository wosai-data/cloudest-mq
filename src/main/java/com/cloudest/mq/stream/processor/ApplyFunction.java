package com.cloudest.mq.stream.processor;

public abstract class ApplyFunction <IN, OUT> extends Function {

    public abstract OUT apply(IN in);

}

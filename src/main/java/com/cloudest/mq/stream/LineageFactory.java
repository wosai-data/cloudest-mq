package com.cloudest.mq.stream;

import com.cloudest.mq.stream.processor.Processor;

public class LineageFactory {

    /**
     * @param inputStream the parent input stream
     * @param processor procesor of child stream used for maintain the lineage of stream transformation
     * @param <T> typeof the child stream
     * @return child stream for the inputStream
     */
    public static <T> DataStream<T> createLineageStream(DataStream inputStream, Processor<T> processor) {
        inputStream.processor.addChild(processor);
        return new DataStream<>(processor);
    }
}

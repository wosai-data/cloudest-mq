package com.cloudest.mq.stream;

import com.cloudest.mq.producer.TopicProducer;
import com.cloudest.mq.serde.KeyValue;
import com.cloudest.mq.stream.processor.*;

/**
 * A DataStream represents a stream of elements of the same type.
 * A DataStream can be transformed to some other DataStream by applying some function
 * Such as filter/apply/sink and so on.
 *
 * TODO more transformation functions to be implemented
 *
 * The @processor field is used to maintain the lineage of the whole DataStream transformations
 *
 * @param <T> The type of the elements in this stream.
 */

public class DataStream<T> {

    public final Processor<?> processor;

    public DataStream(Processor<?> processor) {
        this.processor = processor;
    }

    public DataStream<T> filter(FilterFunction<T> filterFunction) {
        FilterProcessor<T> processor = new FilterProcessor<>("filter", filterFunction);
        return LineageFactory.createLineageStream(this, processor);
    }

    /**
     *  transform DataStream from type T to type R by applying the
     *  @param applyFunction function to do the transformation
     */
    public <R> DataStream<R> apply(ApplyFunction<T, R> applyFunction) {
        Processor apply = new ApplyProcessor<>("apply", applyFunction);
        return LineageFactory.createLineageStream(this, apply);
    }

    public DataStream sink(SinkFunction<T> sinkFunction) {
        Processor<T> sink = new SinkProcessor<>("sink", sinkFunction);
        return LineageFactory.createLineageStream(this, sink);
    }

    public <K, T> DataStream<KeyValue<K, T>> sinkToTopic(TopicProducer<K, T> producer) {
        SinkFunction<KeyValue<K, T>> sinkFunction = new TopicSinkFunction<>(producer);
        Processor<KeyValue<K, T>> sink = new SinkProcessor<>("sink", sinkFunction);
        return LineageFactory.createLineageStream(this, sink);
    }
}

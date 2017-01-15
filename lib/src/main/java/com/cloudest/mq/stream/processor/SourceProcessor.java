package com.cloudest.mq.stream.processor;

public class SourceProcessor <T> extends Processor<SourceEvent> {

    public static class CommitStrategy {

        // TODO more strategy
        public static final int AT_MOST_ONCE = -1;
        public static final int AT_EVERY_ONE = -2; // like exactly once
        public static final int NONE = -2;

    }

    private final SourceFunction<T> sourceFunction;
    private final int strategy;
    private long processedTuples;

    /**
     * @param strategy refer to CommitStrategy.
     *                use strategy greater than 0 as batch commit count. like AT_LEAST_ONCE
     */

    public SourceProcessor(String name, SourceFunction<T> sourceFunction, int strategy) {
        super(name, sourceFunction);
        this.sourceFunction = sourceFunction;
        this.strategy = strategy;
        this.processedTuples = 0;
    }

    @Override public void process(SourceEvent event) {
        if (event == SourceEvent.OPEN) {
            sourceFunction.open();
        } else if (event == SourceEvent.POLL) {
            forwardTuple();
        } else if (event == SourceEvent.COMMIT) {
            sourceFunction.commit();
        } else if (event == SourceEvent.CLOSE) {
            sourceFunction.close();
        }
    }

    private void forwardTuple() {

        if (strategy == CommitStrategy.AT_MOST_ONCE) {
            sourceFunction.commit();
        }

        T tuple = sourceFunction.nextTuple();
        if (tuple != null)
            context().forward(tuple);

        // strategy like EXACTLY_ONCE
        if (strategy == CommitStrategy.AT_EVERY_ONE) {
            sourceFunction.commit();
        }

        processedTuples++;

        // batch commit. strategy like AT_LEAST_ONCE
        if (strategy > 0 && processedTuples % strategy == 0) {
            sourceFunction.commit();
        }
    }

    @Override public void close() {
        sourceFunction.close();
    }

}

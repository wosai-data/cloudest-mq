package com.cloudest.mq.stream.processor;

import com.cloudest.mq.stream.StreamTask;

public class ProcessorContext {

    private final String taskName;
    private final StreamTask task;

    private Processor<?> currentProcessor;

    public ProcessorContext(String taskName, StreamTask task ) {
        this.taskName = taskName;
        this.task = task;
    }

    public String taskName() {
        return taskName;
    }

    public void commit() {
        task.commit();
    }

    public <T> void forward(T tuple) {
        Processor previous = currentProcessor;
        try {
            for (Processor<?> child : currentProcessor.children()) {
                currentProcessor = child;
                ((Processor<T>)child).process(tuple);
            }
        } finally {
            currentProcessor = previous;
        }
    }

    public <T> void forward(T tuple, int childIndex) {
        Processor previousProcessor = currentProcessor;
        final Processor<T> child = (Processor<T>) currentProcessor.children().get(childIndex);
        currentProcessor = child;
        try {
            child.process(tuple);
        } finally {
            currentProcessor = previousProcessor;
        }
    }

    public void setCurrentProcessor(final Processor currentProcessor) {
        this.currentProcessor = currentProcessor;
    }
}


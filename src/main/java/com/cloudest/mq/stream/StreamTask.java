package com.cloudest.mq.stream;

import com.cloudest.mq.consumer.TopicConsumer;
import com.cloudest.mq.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * StreamTask class is used for driving the stream processing logic.
 * Topology is defined by DataStream transformations.
 * the stream lineage and computation logic is maintained in processor
 * @field sourceProcessor is the root processor
 */
public class StreamTask {
    private final String name;
    private final ProcessorContext context;
    private final int LOG_COUNTER = 1000; // configurable?
    private SourceProcessor<?> sourceProcessor = null;
    private AtomicBoolean running;
    private final Thread driver;
    private static final Logger log = LoggerFactory.getLogger(StreamTask.class);

    public class DriverThread implements Runnable {

        private final StreamTask task;

        public DriverThread(StreamTask task) {
            this.task = task;
        }

        @Override public void run() {
            task.driverLoop();
        }
    }

    public StreamTask(String name) {
        this.name = name;
        this.running = new AtomicBoolean(true);
        this.context = new ProcessorContext(name, this);
        this.driver = new Thread(new DriverThread(this));
    }

    /**
     * create datastream from topic consumer
     * @param commitStrategy  refer to class CommitStrategy for much more details
     */
    public <T> DataStream<T> createTopicStream(TopicConsumer<T> topicConsumer, int commitStrategy) {
        if (sourceProcessor != null)
            throw new StreamTaskException(format("Multi source not supported currently, already added a source '%s' ",
                    sourceProcessor.name()));
        SourceProcessor<?> sourceProcessor = new SourceProcessor<>(
                "source-topic-consumer", new TopicSourceFunction<>(topicConsumer), commitStrategy);
        this.sourceProcessor = sourceProcessor;
        return new DataStream<>(sourceProcessor);
    }

    /**
     * main driver loop for stream task
     */
    public void driverLoop() {
        log.info("Stream task {} starting", name);

        initContext(sourceProcessor);

        sourceProcessor.process(SourceEvent.OPEN);
        context.setCurrentProcessor(sourceProcessor);

        long tuples = 0;
        while (isRunning()) {
            sourceProcessor.process(SourceEvent.POLL);
            if (++tuples % LOG_COUNTER == 0)
                log.info("Stream task processed {} tuple from source", tuples);
        }

        log.info("Stream task {} exit driver loop", name);
        sourceProcessor.process(SourceEvent.CLOSE);
        log.info("Stream task {} end processing with {} records", name, tuples);
    }

    /**
     * recursively init context for processor
     * @param processor
     */
    private void initContext(Processor<?> processor) {
        processor.init(context);

        for (Processor<?> child : processor.children())
            initContext(child);
    }

    public void commit() {
        sourceProcessor.process(SourceEvent.COMMIT);
    }

    private boolean isRunning() {
        if (!running.get()) {
            log.info("Stream task {} has been cancelled by user.", name);
            return false;
        }
        return true;
    }

    public void start() {
        if (sourceProcessor == null)
            throw new StreamTaskException(format("No source processor created for stream task '%s' ", name));
        driver.start();
    }

    public void stop() {
        log.info("Stopping stream task");
        running.set(false);
        driver.interrupt();
        try {
            driver.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.warn("Failed to join driver thread", e);
        }
        log.info("Stream task {} stopped", name);
    }
}

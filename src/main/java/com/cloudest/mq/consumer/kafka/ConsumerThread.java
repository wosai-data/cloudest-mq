package com.cloudest.mq.consumer.kafka;

import com.cloudest.mq.common.MessagingException;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * main thread for processing all KakfaConsumer related logic
 */
public class ConsumerThread extends Thread {

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String consumerId;
    private final String topic;
    private final AtomicBoolean running;
    private final long pollTimeoutMs;
    private final long pollIntervalMs;
    private final TopicRecordQueue recordQueue;
    private final AtomicBoolean needCommit;
    private Throwable rebalanceException = null;
    private CountDownLatch pendingCommit = null;
    private static final Logger log = LoggerFactory.getLogger(ConsumerThread.class);
    private final AtomicBoolean isPolling = new AtomicBoolean(false);
    
    public ConsumerThread(String topic, String appId, TopicRecordQueue recordQueue, Properties properties) {
        this.topic = topic;
        // TODO make this configurable
        this.pollTimeoutMs = Long.parseLong(properties.getProperty("cloudest.mq.consumer.kafka.poll.timeout.ms", "30000"));
        this.pollIntervalMs = Long.parseLong(properties.getProperty("cloudest.mq.consumer.kafka.poll.interval.ms", "200000"));
        this.recordQueue = recordQueue;
        this.running = new AtomicBoolean(true);
        this.needCommit = new AtomicBoolean(false);

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Long.toString(this.pollIntervalMs + this.pollTimeoutMs));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // XXX set client id is good for debugging
        this.consumerId = Thread.currentThread().getName() + "@" + ManagementFactory.getRuntimeMXBean().getName();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
        
        this.consumer = new KafkaConsumer<>(properties);

    }

    final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assignment) {
            try {
                log.info("New partitions '{}' assigned to the consumer '{}'.", assignment, consumerId);

                for (TopicPartition partition : assignment) {
                    recordQueue.assignPartition(partition);
                }
            } catch (Throwable t) {
                rebalanceException = t;
                throw t;
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
            try {
                log.info("Partitions '{}' revoked from the consumer.", revoked);

                for (TopicPartition partition : revoked) {
                    recordQueue.revokePartition(partition);
                }
            } catch (Throwable t) {
                rebalanceException = t;
                throw t;
            }
        }
    };

    public void close() {
        log.info("Close kafka consumer thread");
        running.set(false);
    }

    @Override
    public void run() {
        log.info("Consumer thread {} starting.", consumerId);
        consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
        try {
            processLoop();
        } catch (KafkaException e) {
            log.error("KafkaException occured during processing: ", e);
            throw e;
        } catch (Exception e) {
            log.error("Streams application error during processing: ", e);
            throw e;
        }
    }

    private void processLoop() {

        while (isRunning()) {
            // XXX all kafka consumer message should be processed in the same thread.
            // TODO refactor this
            if (needCommit.get()) {
                commitOffsets();
            }

            if (recordQueue.isBelowRedzone()) {
                consumer.resume(recordQueue.assignedPartitions());
            }

            try {
                long timeout = this.pollTimeoutMs;

                isPolling.set(true);
                ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
                isPolling.set(false);

                if (rebalanceException != null)
                    throw new MessagingException("Failed to rebalance", rebalanceException);

                if (!records.isEmpty()) {
                    for (TopicPartition partition : records.partitions()) {
                        int count = recordQueue.addBatch(records.records(partition));
                        log.info("Polled {} records from partition {}", count, partition);
                    }
                    if (recordQueue.isFull()) {
                        consumer.pause(recordQueue.assignedPartitions());
                    }
                }
            }catch(WakeupException we) {
                // This is essentially an interrupt exception.
            }
        }

        log.warn("ConsumerThread end processing");
        try {
            consumer.close();
        } catch (Throwable e) {
            log.error("Failed to close consumer", e);
        }
        log.info("Successfully closed KafkaConsumer");
    }

    private boolean isRunning() {
        if (!running.get()) {
            log.debug("Consumer has been closed by user.");
            return false;
        }
        return true;
    }

    public void commit() throws InterruptedException {
        pendingCommit = new CountDownLatch(1);
        needCommit.set(true);
        wakeupBlockingPoll();
        pendingCommit.await();
    }

    public void wakeupBlockingPoll() {
        if (isPolling.get()) {
            // The blocking poll might have finished at this moment and we end up with a misfired wakeup call.
            consumer.wakeup();
        }
    }
    private void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        while(true) {
            try {
                consumer.commitSync(offsets);
                log.info("Successfully committed {} offsets", offsets.size());
                break;
            }catch(WakeupException ex) {
                // retry
            }
        }
    }

    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsets = recordQueue .latestConsumedOffsets();
        log.info("Begin to commit consumed offsets {}", offsets);
        // batch commit all partitions. XXX maybe we can do an increase commit?
        commitSync(offsets);
        needCommit.set(false);
        pendingCommit.countDown();
    }
}

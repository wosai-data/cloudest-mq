package com.cloudest.mq.consumer.kafka;

import com.cloudest.mq.common.MessagingException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TopicRecordQueue for buffering records of all partitions of the topic consumer.
 */
public class TopicRecordQueue {

    public class PartitionInfo {

        private AtomicLong latestConsumedOffset = null;

        public PartitionInfo() {
            latestConsumedOffset = new AtomicLong(-1);
        }

        /**
         * @return whether this partition has record been consumed by user
         * use latestConsumedOffset is enough
         */
        public boolean isConsumed() {
            return latestConsumedOffset.get() != -1;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TopicRecordQueue.class);
    private final String topic;
    private final ReadWriteLock partInfoLock = new ReentrantReadWriteLock();
    private final Map<Integer, PartitionInfo> partitionInfos;
    private final BlockingQueue<ConsumerRecord<byte[], byte[]>> queue;
    private int softCapacity = 5000;
    private int belowRedzone = 3750;
    private ConsumerThread ioThread;

    public TopicRecordQueue(String topic, int softCapacity) {
        this.topic = topic;
        this.partitionInfos = new HashMap<>();
        if (softCapacity > 0) {
            this.softCapacity = softCapacity;
            this.belowRedzone = softCapacity * 3 / 4;
        }
        this.queue = new LinkedBlockingQueue<>();
    }

    public void setIOThread(ConsumerThread thread) {
        this.ioThread = thread;
    }

    public void assignPartition(TopicPartition partition) {
        partInfoLock.writeLock().lock();
        try {
            log.info("New partition '{}' assigned to queue", partition);
            partitionInfos.put(partition.partition(), new PartitionInfo());
        } finally {
            partInfoLock.writeLock().unlock();
        }
    }

    public List<TopicPartition> assignedPartitions() {
        List<TopicPartition> partitions = new ArrayList<>();
        for(Integer partition:  partitionInfos.keySet()) {
            partitions.add(new TopicPartition(topic, partition));
        }
        return partitions;
    }
    public void revokePartition(TopicPartition partition) {
        partInfoLock.writeLock().lock();
        try {
            log.info("Partition '{}' revoked from queue", partition);
            PartitionInfo partInfo = partitionInfos.remove(partition.partition());
            if (partInfo == null) {
                throw new MessagingException(
                        "Partition does not assigned to this consumer before:" + partition);
            }
        } finally {
            partInfoLock.writeLock().unlock();
        }
    }

    private boolean partitionVailid(int partition) {
        partInfoLock.readLock().lock();
        try {
            return partitionInfos.containsKey(partition);
        } finally {
            partInfoLock.readLock().unlock();
        }
    }

    private void rememberLatestConsumed(int partition, long offset) {
        partInfoLock.readLock().lock();
        try {
            PartitionInfo partInfo = partitionInfos.get(partition);
            if (partInfo == null) {
                throw new MessagingException(
                        "Partition does not assigned to this consumer before:" + partition);
            }
            partInfo.latestConsumedOffset.set(offset);
        } finally {
            partInfoLock.readLock().unlock();
        }
    }

    private void wakeupIOThreadIfNecessary() {
        if (isBelowRedzone()) {
            ioThread.wakeupBlockingPoll();
        }
    }

    public ConsumerRecord<byte[], byte[]> poll() throws InterruptedException {

        wakeupIOThreadIfNecessary();

        while (true) {
            ConsumerRecord<byte[], byte[]> record = queue.take();
            
            wakeupIOThreadIfNecessary();

            if (!partitionVailid(record.partition())) {
                log.warn("Partition '{}' maybe revoked", record.partition());
            }else {
                rememberLatestConsumed(record.partition(), record.offset());
                return record;
            }
        }
    }

    /**
     * Add a batch of  polled ConsumerRecords into the buffer queue
     *
     * @param records the batch
     * @return the size of the batch
     */
    public int addBatch(Iterable<ConsumerRecord<byte[], byte[]>> records) {

        int count = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            queue.offer(record);
            count++;
        }
        return count;
    }

    public boolean isFull() {
        return queue.size() >= softCapacity; 
    }
    
    public boolean isBelowRedzone() {
        return queue.size() < belowRedzone;
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public Map<TopicPartition, OffsetAndMetadata> latestConsumedOffsets() {
        partInfoLock.readLock().lock();
        try {
            Map<TopicPartition, OffsetAndMetadata> latestOffsets = new HashMap<>();
            for (Map.Entry<Integer, PartitionInfo> entry : partitionInfos.entrySet()) {
                TopicPartition partition = new TopicPartition(topic, entry.getKey());
                if (entry.getValue().isConsumed()) {
                    // XXX manually plus offset by one for commit
                    long offset = entry.getValue().latestConsumedOffset.get() + 1;
                    latestOffsets.put(partition, new OffsetAndMetadata(offset));
                    log.debug("Latest consumed offset of Partition '{}' is '{}'", partition, offset);
                }
            }
            return latestOffsets;
        } finally {
            partInfoLock.readLock().unlock();
        }
    }
}


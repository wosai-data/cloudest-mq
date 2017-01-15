package com.cloudest.connect;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apply 1-to-1 transform on the input stream and send the transformed records into a writer.
 * @author dun
 *
 * @param <IK>
 * @param <IV>
 * @param <OK>
 * @param <OV>
 */
public class KafkaTransformer2<IK, IV, OK, OV> extends KafkaStreamProcessor<IK, IV> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTransformer2.class);

    private Map<Integer, BlockingQueue<OffsetStatus>> pendingByPartition;

    private RecordWriter<OK, OV> writer;
    
    private TransformFunc<IK, IV, OK, OV> function;
    
    
    private int pendingCapacity = 500;

    private boolean resetOffset;

    public KafkaTransformer2(String groupId,
                             String inBrokerList, String inTopic, int[] inPartitions,
                             Class<? extends Deserializer<IK>> keyDeserializer,
                             Class<? extends Deserializer<IV>> valueDeserializer,
                             Properties config,
                             FilterFunc<IK, IV> include,
                             TransformFunc<IK, IV, OK, OV> function,
                             RecordWriter<OK, OV> writer,
                             boolean resetOffset) {

        super(groupId, inBrokerList, inTopic, inPartitions, keyDeserializer, valueDeserializer, config, include);

        this.resetOffset = resetOffset;
        this.function = function;
        this.writer = writer;

        initializePendingQueues();
    }

    private void initializePendingQueues() {
        pendingByPartition = new ConcurrentHashMap<>();
        for(int partition: inPartitions) {
            pendingByPartition.put(partition, new ArrayBlockingQueue<OffsetStatus>(pendingCapacity));
        }
    }

    private void doCommitIfNecessary() {
        Map<TopicPartition, OffsetAndMetadata> offsets = null;
        for(Map.Entry<Integer, BlockingQueue<OffsetStatus>> entry: pendingByPartition.entrySet()) {
            int partition = entry.getKey();
            BlockingQueue<OffsetStatus> pendingQueue = entry.getValue();
            OffsetAndMetadata offset = createPartitionCommit(pendingQueue);
            if (offset != null) {
                if (offsets == null) {
                    offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                }
                offsets.put(new TopicPartition(inTopic, partition), offset);
            }
        }
        if (offsets != null) {
            kafkaConsumer.commitAsync(offsets, null);
        }
    }

    private OffsetAndMetadata createPartitionCommit(BlockingQueue<OffsetStatus> pendingQueue) {
        long offset = -1;
        int count = 0;
        for (OffsetStatus status: pendingQueue) {
            if (status.isConsumed()) {
                offset = status.getOffset();
                ++count;
            }else{
                break;
            }
        }
        if (offset > 0) {
            for (int i=0; i<count; ++i) {
                pendingQueue.poll();
            }
            return new OffsetAndMetadata(offset+1);
        }else{
            return null;
        }
    }

    class DefaultRecordWriterCallback implements RecordWriterCallback{
        OffsetStatus offsetStatus;

        DefaultRecordWriterCallback(OffsetStatus offsetStatus) {
            this.offsetStatus = offsetStatus;
        }
        
        @Override
        public void onSuccess() {
            this.offsetStatus.setConsumed(true);
        }

        @Override
        public void onError(Exception ex) {
            new Thread(new Runnable() {
                
                @Override
                public void run() {
                    shutdown();
                }
            }, "producer-error-panic-thread").start();
            
        }
    }

    private OffsetStatus addPending(ConsumerRecord<IK, IV> record) throws InterruptedException {
        OffsetStatus offsetStatus = new OffsetStatus(record.offset());
        BlockingQueue<OffsetStatus> pendingQueue = pendingByPartition.get(record.partition());
        pendingQueue.put(offsetStatus);
        return offsetStatus;
    }

    @Override
    protected void initPartitions() {

        if (resetOffset) {
            logger.debug("reset offsets to beggining of assigned partitions");
            kafkaConsumer.seekToBeginning(getInTopicPartitions());
        }
        
    }

    @Override
    protected void prePolling() {
        doCommitIfNecessary();
    }

    @Override
    protected void preIOThreadExit() {
        doCommitIfNecessary();
        
    }

    @Override
    protected void preMainThreadExit() {
        writer.close();
    }

    @Override
    protected void process(ConsumerRecord<IK, IV> cr) throws InterruptedException {
        Record<OK, OV> record = function.apply(new Record<IK, IV>(cr.key(), cr.value()));
        if (record != null) {
            
            OffsetStatus offsetStatus = addPending(cr);
            writer.write(record, new DefaultRecordWriterCallback(offsetStatus));
        }
    }

}

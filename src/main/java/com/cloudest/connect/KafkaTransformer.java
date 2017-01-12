package com.cloudest.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTransformer<IK, IV, OK, OV> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTransformer.class);

    private BlockingQueue<ConsumerRecord<IK, IV>> inputQueue;
    private Map<Integer, BlockingQueue<OffsetStatus>> pendingByPartition;

    private KafkaConsumer<IK, IV> kafkaConsumer;
    private RecordWriter<OK, OV> writer;
    
    private FilterFunc<IK, IV> include;
    private TransformFunc<IK, IV, OK, OV> function;
    
    private InputIOThread inputIOThread;
    private Thread mainThread;
    
    private String groupId;
    private String inBrokerList;
    private String inTopic;
    private int[] inPartitions;
    private Class<? extends Deserializer<IK>> keyDeserializer;
    private Class<? extends Deserializer<IV>> valueDeserializer;
    private Properties otherProps;
    
    private volatile boolean shouldExit = false;
    private long pollingTimeoutMs = 2000;

    private int inputQueueCapacity = 5000;
    private int pendingCapacity = 500;
    
    public KafkaTransformer(String groupId,
                            String inBrokerList, String inTopic, int[] inPartitions,
                            Class<? extends Deserializer<IK>> keyDeserializer,
                            Class<? extends Deserializer<IV>> valueDeserializer,
                            Properties config,
                            FilterFunc<IK, IV> include,
                            TransformFunc<IK, IV, OK, OV> function,
                            RecordWriter<OK, OV> writer) {

        this.groupId = groupId;
        this.inBrokerList = inBrokerList;
        this.inTopic = inTopic;
        this.inPartitions = inPartitions;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.otherProps = config;
        

        this.include = include;
        this.function = function;
        this.writer = writer;

        kafkaConsumer = new KafkaConsumer<>(createConsumerProps());

        inputQueue = new ArrayBlockingQueue<>(inputQueueCapacity*2);

        initializePendingQueues();
        
        inputIOThread = new InputIOThread();
    }

    private Properties createConsumerProps() {
        Properties props = new Properties();
        props.putAll(this.otherProps);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, inBrokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        return props;
    }

    private void initializePendingQueues() {
        pendingByPartition = new ConcurrentHashMap<>();
        for(int partition: inPartitions) {
            pendingByPartition.put(partition, new ArrayBlockingQueue<OffsetStatus>(pendingCapacity));
        }
    }

    private List<TopicPartition> getInTopicPartitions() {
        List<TopicPartition> tps = new ArrayList<>();
        for (int p: inPartitions) {
            tps.add(new TopicPartition(inTopic, p));
        }
        return tps;
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

    private boolean isInputQueueFull() {
        return inputQueue.size() >= inputQueueCapacity;
    }
    private boolean isInputQueueHealthy() {
        return inputQueue.size() <= inputQueueCapacity/2;
    }

    private boolean shouldInclude(IK key, IV value) {
        if (include == null) {
            return true;
        }else{
            return include.apply(key, value);
        }
    }

    class InputIOThread extends Thread {
        volatile boolean shouldClose = false;

        @Override
        public void run() {
            List<TopicPartition> tps = getInTopicPartitions();
            for (TopicPartition tp: tps) {
                OffsetAndMetadata offset = kafkaConsumer.committed(tp);
                if (offset == null) {
                    logger.debug("{} has no prior commit", tp);
                }else {
                    logger.debug("{} has committed offset {}", tp, offset);
                }
            }
            kafkaConsumer.assign(getInTopicPartitions());
            while(!shouldClose) {
                doCommitIfNecessary();
                
                if (isInputQueueHealthy()) {
                    kafkaConsumer.resume(getInTopicPartitions());
                }
                ConsumerRecords<IK, IV> records = kafkaConsumer.poll(pollingTimeoutMs);
                for(ConsumerRecord<IK, IV> record: records) {
                    if (!shouldInclude(record.key(), record.value())) {
                        continue;
                    }
                    try {
                        logger.debug("adding {} to input queue", record);
                        inputQueue.put(record);
                    } catch (InterruptedException e) {
                        // this is not a possible execution path.
                    }
                }
                
                if (isInputQueueFull()) {
                    kafkaConsumer.pause(getInTopicPartitions());
                }
                
            }

            logger.debug("exit IO thread because shouldClose=true");
            doCommitIfNecessary();
            logger.debug("committed those that are secured in the output topic.");
            kafkaConsumer.close();
            logger.debug("consumer gracefully closed");
        }
        
        public void close() {
            logger.debug("flag shouldClose for the IO thread");
            this.shouldClose = true;
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

    public void start() {
        inputIOThread.start();
        mainThread = Thread.currentThread();
        while(!shouldExit) {
            try {
                ConsumerRecord<IK, IV> cr = inputQueue.take();
                logger.debug("processing {}", cr);
                Record<OK, OV> record = function.apply(new Record<IK, IV>(cr.key(), cr.value()));
                if (record != null) {
                    
                    OffsetStatus offsetStatus = addPending(cr);
                    writer.write(record, new DefaultRecordWriterCallback(offsetStatus));
                }
            } catch (InterruptedException e) {
                // continue while loop
                logger.debug("main thread is interrupted during blocking IO");
            }
        }
        logger.debug("ensure all sent messages are secured in the output topic.");
        writer.close();
        logger.debug("producer gracefully closed.");

        inputIOThread.close();
        logger.debug("waiting for IO thread to exit.");
        try {
            inputIOThread.join();
        } catch (InterruptedException e) {
            logger.error("main thread interrupted during wait", e);
        }
    }

    /**
     * You should call this method from threads other than the main.
     */
    public void shutdown() {
        logger.debug("flag shouldExit for the main thread and interrupt it.");

        shouldExit = true;
        mainThread.interrupt();
        try {
            mainThread.join();
        } catch (InterruptedException e) {
            // this is not possible execution path.
            logger.error("shutdown hook thread interrupted during wait", e);
        }
    }
}

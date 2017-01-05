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

public class KafkaTransformer<IK, IV, OK, OV> {
    private BlockingQueue<ConsumerRecord<IK, IV>> inputQueue;
    private Map<Integer, BlockingQueue<OffsetStatus>> pendingByPartition;

    private KafkaConsumer<IK, IV> kafkaConsumer;
    private KafkaProducer<OK, OV> kafkaProducer;
    
    private TransformFunc<IK, IV, OK, OV> function;
    
    private InputIOThread inputIOThread;
    private Thread mainThread;
    
    private String groupId;
    private String inBrokerList;
    private String inTopic;
    private List<Integer> inPartitions;
    private String outBrokerList;
    private String outTopic;
    private Class<Deserializer<IK>> keyDeserializer;
    private Class<Deserializer<IV>> valueDeserializer;
    private Class<Serializer<OK>> keySerializer;
    private Class<Serializer<OV>> valueSerializer;
    private Class<Partitioner> partitioner;
    private Properties otherProps;
    
    private volatile boolean shouldExit = false;
    private long pollingTimeoutMs = 2000;

    private int inputQueueCapacity = 5000;
    private int pendingCapacity = 500;
    
    public KafkaTransformer(String groupId,
                            String inBrokerList, String inTopic, List<Integer> inPartitions,
                            String outBrokerList, String outTopic,
                            Class<Deserializer<IK>> keyDeserializer,
                            Class<Deserializer<IV>> valueDeserializer,
                            Class<Serializer<OK>> keySerializer,
                            Class<Serializer<OV>> valueSerializer,
                            Class<Partitioner> partitioner,
                            Properties config,
                            TransformFunc<IK, IV, OK, OV> function) {

        this.groupId = groupId;
        this.inBrokerList = inBrokerList;
        this.inTopic = inTopic;
        this.inPartitions = inPartitions;
        this.outBrokerList = outBrokerList;
        this.outTopic = outTopic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.partitioner = partitioner;
        this.otherProps = config;
        
        if (this.outBrokerList == null) {
            this.outBrokerList = inBrokerList;
        }

        this.function = function;

        kafkaConsumer = new KafkaConsumer<>(createConsumerProps());

        kafkaProducer = new KafkaProducer<>(createProducerProps());

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

    private Properties createProducerProps() {
        Properties props = new Properties();
        props.putAll(this.otherProps);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, outBrokerList);
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 600000);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return props;
    }

    private void initializePendingQueues() {
        pendingByPartition = new ConcurrentHashMap<>();
        for(Integer partition: inPartitions) {
            pendingByPartition.put(partition, new ArrayBlockingQueue<OffsetStatus>(pendingCapacity));
        }
    }

    private List<TopicPartition> getInTopicPartitions() {
        List<TopicPartition> tps = new ArrayList<>();
        for (Integer p: inPartitions) {
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
        for (OffsetStatus status: pendingQueue) {
            if (status.isConsumed()) {
                offset = status.getOffset();
            }else{
                break;
            }
        }
        if (offset > 0) {
            return new OffsetAndMetadata(offset);
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

    class InputIOThread extends Thread {
        volatile boolean shouldClose = false;

        @Override
        public void run() {
            kafkaConsumer.assign(getInTopicPartitions());
            while(!shouldClose) {
                doCommitIfNecessary();
                
                if (isInputQueueHealthy()) {
                    kafkaConsumer.resume(getInTopicPartitions());
                }
                ConsumerRecords<IK, IV> records = kafkaConsumer.poll(pollingTimeoutMs);
                for(ConsumerRecord<IK, IV> record: records) {
                    try {
                        inputQueue.put(record);
                    } catch (InterruptedException e) {
                        // this is not a possible execution path.
                    }
                }
                
                if (isInputQueueFull()) {
                    kafkaConsumer.pause(getInTopicPartitions());
                }
                
            }

            doCommitIfNecessary();
            kafkaConsumer.close();
        }
        
        public void close() {
            this.shouldClose = true;
        }
    }
    
    class ProducerCallback implements Callback {
        OffsetStatus offsetStatus;

        ProducerCallback(OffsetStatus offsetStatus) {
            this.offsetStatus = offsetStatus;
        }
        
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                this.offsetStatus.setConsumed(true);
            }else {
                // Report a critical error and shutdown. We rely on external task manager to restart the transformer when the critial error is resolved.
                shutdown();
            }
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
                Record<OK, OV> record = function.apply(new Record<IK, IV>(cr.key(), cr.value()));
                if (record != null) {
                    ProducerRecord<OK, OV> pr = new ProducerRecord<>(outTopic, record.getKey(), record.getValue());
                    
                    OffsetStatus offsetStatus = addPending(cr);
                    kafkaProducer.send(pr, new ProducerCallback(offsetStatus));
                }
            } catch (InterruptedException e) {
                // continue while loop
            }
        }
        kafkaProducer.close();

        inputIOThread.close();
        
        try {
            inputIOThread.join();
        } catch (InterruptedException e) {
        }
    }

    /**
     * You should call this method from threads other than the main.
     */
    public void shutdown() {
        shouldExit = true;
        mainThread.interrupt();
    }
}

package com.cloudest.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaStreamProcessor<IK, IV> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamProcessor.class);

    protected BlockingQueue<ConsumerRecord<IK, IV>> inputQueue;

    protected KafkaConsumer<IK, IV> kafkaConsumer;
    
    protected FilterFunc<IK, IV> include;
    
    protected InputIOThread inputIOThread;
    protected ProcessorThread processorThread;
    
    protected String groupId;
    protected String inBrokerList;
    protected String inTopic;
    protected int[] inPartitions;
    protected Class<? extends Deserializer<IK>> keyDeserializer;
    protected Class<? extends Deserializer<IV>> valueDeserializer;
    protected Properties otherProps;
    
    private volatile boolean shouldExit = false;
    private long pollingTimeoutMs = 2000;

    private int inputQueueCapacity = 5000;

    public KafkaStreamProcessor(String groupId,
                                String inBrokerList, String inTopic, int[] inPartitions,
                                Class<? extends Deserializer<IK>> keyDeserializer,
                                Class<? extends Deserializer<IV>> valueDeserializer,
                                Properties config,
                                FilterFunc<IK, IV> include) {

        this.groupId = groupId;
        this.inBrokerList = inBrokerList;
        this.inTopic = inTopic;
        this.inPartitions = inPartitions;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.otherProps = config;
        

        this.include = include;

        kafkaConsumer = new KafkaConsumer<>(createConsumerProps());

        inputQueue = new ArrayBlockingQueue<>(inputQueueCapacity*2);

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

    protected List<TopicPartition> getInTopicPartitions() {
        List<TopicPartition> tps = new ArrayList<>();
        for (int p: inPartitions) {
            tps.add(new TopicPartition(inTopic, p));
        }
        return tps;
    }

    protected boolean isInputQueueFull() {
        return inputQueue.size() >= inputQueueCapacity;
    }
    protected boolean isInputQueueHealthy() {
        return inputQueue.size() <= inputQueueCapacity/2;
    }

    protected boolean shouldInclude(IK key, IV value) {
        if (include == null) {
            return true;
        }else{
            return include.apply(key, value);
        }
    }

    protected abstract void initPartitions();
    protected abstract void prePolling();
    protected abstract void preIOThreadExit();
    protected abstract void preProcessorThreadExit();
    protected abstract void process(ConsumerRecord<IK, IV> record) throws InterruptedException;

    class InputIOThread extends Thread {
        volatile boolean shouldClose = false;

        @Override
        public void run() {
            List<TopicPartition> tps = getInTopicPartitions();
            kafkaConsumer.assign(tps);

            for (TopicPartition tp: tps) {
                OffsetAndMetadata offset = kafkaConsumer.committed(tp);
                if (offset == null) {
                    logger.debug("{} has no prior commit", tp);
                }else {
                    logger.debug("{} has committed offset {}", tp, offset);
                }
            }

            initPartitions();

            while(!shouldClose) {
                prePolling();
                
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

            logger.debug("exit IO thread shouldClose = true");
            preIOThreadExit();
            kafkaConsumer.close();
            logger.debug("kafka consumer closed gracefully");
        }
        
        public void close() {
            logger.debug("flag shouldClose = true to end the IO thread");
            this.shouldClose = true;
        }
    }
    

    class ProcessorThread extends Thread {
        @Override
        public void run() {
            while(!shouldExit) {
                try {
                    ConsumerRecord<IK, IV> cr = inputQueue.take();
                    logger.debug("processing {}", cr);
                    process(cr);
                } catch (InterruptedException e) {
                    // continue while loop
                    logger.debug("processor thread is interrupted during blocking IO");
                }
            }

            preProcessorThreadExit();

            inputIOThread.close();
            logger.debug("waiting for IO thread to exit.");
            try {
                inputIOThread.join();
            } catch (InterruptedException e) {
                logger.error("processor thread interrupted during wait", e);
            }
            
        }
    }

    public Thread start() {
        inputIOThread = new InputIOThread();
        inputIOThread.start();
        processorThread = new ProcessorThread();
        processorThread.start();
        return processorThread;
    }

    /**
     * You should call this method from threads other than the processor thread.
     */
    public void shutdown() {
        logger.debug("flag shouldExit for the processor thread and interrupt it.");

        shouldExit = true;
        processorThread.interrupt();
        
    }

    public void processorWait() {
        try {
            processorThread.join();
        } catch (InterruptedException e) {
            // this is not possible execution path.
            logger.error("shutdown hook thread interrupted during wait", e);
        }
        
    }

    public void shutdownAndWait() {
        shutdown();
        processorWait();
    }
}

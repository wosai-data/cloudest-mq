package com.cloudest.connect;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaTransformer<IK, IV, OK, OV> {
    private BlockingQueue<ConsumerRecord<IK, IV>> inputQueue;
    private Map<Integer, BlockingQueue<OffsetStatus>> pendingByPartition;

    private KafkaConsumer<IK, IV> kafkaConsumer;
    private KafkaProducer<OK, OV> kafkaProducer;
    
    public KafkaTransformer(String groupId,
                            String inBrokerList, String inTopic, List<Integer> inPartitions,
                            String outBrokerList, String outTopic,
                            Class<Deserializer<IK>> keyDeserializer,
                            Class<Deserializer<IV>> valueDeserializer,
                            Class<Serializer<OK>> keySerializer,
                            Class<Serializer<OV>> valueSerializer,
                            Properties config) {

        if (outBrokerList == null) {
            outBrokerList = inBrokerList;
        }

        Properties props = new Properties();
        props.putAll(config);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, inBrokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        
        kafkaConsumer = new KafkaConsumer<>(props);

        Properties outProps = new Properties();
        outProps.putAll(config);
        outProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, outBrokerList);
        outProps.put(ProducerConfig.RETRIES_CONFIG, 5);
        outProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 600000);
        kafkaProducer = new KafkaProducer<>(outProps);
        
        
    }
}

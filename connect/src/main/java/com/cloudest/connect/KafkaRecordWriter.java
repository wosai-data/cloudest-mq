package com.cloudest.connect;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaRecordWriter<K, V> implements RecordWriter<K, V> {

    private String brokerList;
    private String topic;
    private Class<? extends Serializer<K>> keySerializer;
    private Class<? extends Serializer<? super V>> valueSerializer;
    private Class<? extends Partitioner> partitioner;
    private Properties otherProps;
    private KafkaProducer<K, V> producer;

    public KafkaRecordWriter(String brokerList, String topic,
                             Class<? extends Serializer<K>> keySerializer,
                             Class<? extends Serializer<? super V>> valueSerializer,
                             Class<? extends Partitioner> partitioner,
                             Properties config) {

        this.brokerList = brokerList;
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.partitioner = partitioner;
        this.otherProps = config;
        
        this.producer = new KafkaProducer<K, V>(createProducerProps());
    }

    @Override
    public void write(Record<K, V> record, RecordWriterCallback callback) {
        producer.send(new ProducerRecord<K, V>(topic, record.getKey(), record.getValue()), new ProducerCallback(callback));
    }

    private Properties createProducerProps() {
        Properties props = new Properties();
        props.putAll(this.otherProps);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 600000);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return props;
    }

    class ProducerCallback implements Callback {
        RecordWriterCallback recordWriterCallback;

        ProducerCallback(RecordWriterCallback recordWriterCallback) {
            this.recordWriterCallback = recordWriterCallback;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null)  {
                this.recordWriterCallback.onSuccess();
            } else {
                this.recordWriterCallback.onError(exception);
            }
        }
    }

    @Override
    public void close() {
        producer.close();
        
    }
}

package com.cloudest.mq.util;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class KafkaConsumerBuilder {

	public static KafkaConsumerBuilder builder(String brokers) {
		return new KafkaConsumerBuilder(brokers);
	}

	private String brokers;
	private String groupId;
	private boolean autoCommit = false;
	private int autoCommitInterval = 0;
	private String initialOffset = "latest";
	private Class<?> keyDeserializer = ByteArrayDeserializer.class;
	private Class<?> valueDeserializer = ByteArrayDeserializer.class;

	public KafkaConsumerBuilder(String brokers) {
		this.brokers = brokers;
	}

	public KafkaConsumerBuilder groupId(String groupId) {
		this.groupId = groupId;
		return this;
	}
	public KafkaConsumerBuilder autoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
		return this;
	}
	public KafkaConsumerBuilder initialOffset(String initialOffset) {
		this.initialOffset = initialOffset;
		return this;
	}
	/*
	public KafkaConsumerBuilder keyDeserializer(Class<?> keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
		return this;
	}
	public KafkaConsumerBuilder valueDeserializer(Class<?> valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
		return this;
	}
	*/
	public KafkaConsumer<byte[], byte[]> build() throws BuilderException {
        Properties props = new Properties();
        if (brokers == null) {
        	throw new BuilderException("missing broker list");
        }
        props.put("bootstrap.servers", brokers);
        if (groupId != null) {
        	props.put("group.id", groupId);
        }
        props.put("enable.auto.commit", autoCommit);
        if (autoCommitInterval > 0) {
        	props.put("auto.commit.interval.ms", autoCommitInterval);
        }

        props.put("auto.offset.reset", initialOffset);
        props.put("key.deserializer", keyDeserializer.getName());
        props.put("value.deserializer", valueDeserializer.getName());

        return new KafkaConsumer<>(props);
	}
}

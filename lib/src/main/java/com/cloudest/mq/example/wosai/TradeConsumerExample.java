package com.cloudest.mq.example.wosai;

import com.cloudest.mq.consumer.TopicConsumer;
import com.cloudest.mq.consumer.kafka.KafkaTopicConsumer;
import com.cloudest.mq.producer.TopicProducer;
import com.cloudest.mq.producer.kafka.KafkaTopicProducer;
import com.cloudest.mq.serde.ByteArraySerde;
import com.cloudest.mq.serde.json.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class TradeConsumerExample {

    private static void help() {
        System.err.println("Usage: TradeConsumerExample <bootstrap.brokers> <appId> <topic>");
        System.exit(-1);
    }

    public static Properties createKafkaProperties(String brokers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return props;
    }

    private static void produceTradeMessageTo(String topic, String brokers) {

        TopicProducer<byte[], TradeMessage> producer = new KafkaTopicProducer<>(
                topic, new ByteArraySerde(), new JsonSerde<>(TradeMessage.class),
                createKafkaProperties(brokers));

        int i = 0;
        while (i++ < 10) {
            TradeMessage message = new TradeMessage();
            if (i % 2 == 0)
                message.setRevoke();
            message.tsn = Integer.toString(i);
            message.amount = Integer.toString(100 * i + i);
            message.order_ctime = System.currentTimeMillis();
            producer.post(null, message);
        }
        producer.flush();
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {

        if (args.length != 3) {
            help();
        }

        String brokers = args[0];
        String appId = args[1];
        String topic = args[2];

        produceTradeMessageTo(topic, brokers);

        TopicConsumer<TradeMessage> consumer = new KafkaTopicConsumer<>(
                topic, appId, new JsonSerde<>(TradeMessage.class), createKafkaProperties(
                brokers));
        consumer.open();
        while (true) {
            TradeMessage message = consumer.getNext();
            if (message == null) {
                System.out.println("message is null");
            } else {
                System.out.println("event is: " + message.event
                        + ", amount is:"  + message.amount
                        + ", tsn is " + message.tsn);
            }
            consumer.commit();
        }
    }
}

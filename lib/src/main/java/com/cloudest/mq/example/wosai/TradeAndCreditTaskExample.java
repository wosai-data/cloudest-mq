package com.cloudest.mq.example.wosai;

import com.cloudest.mq.consumer.TopicConsumer;
import com.cloudest.mq.consumer.kafka.KafkaTopicConsumer;
import com.cloudest.mq.producer.TopicProducer;
import com.cloudest.mq.producer.kafka.KafkaTopicProducer;
import com.cloudest.mq.serde.ByteArraySerde;
import com.cloudest.mq.serde.KeyValue;
import com.cloudest.mq.serde.json.JsonSerde;
import com.cloudest.mq.stream.DataStream;
import com.cloudest.mq.stream.StreamTask;
import com.cloudest.mq.stream.processor.ApplyFunction;
import com.cloudest.mq.stream.processor.FilterFunction;
import com.cloudest.mq.stream.processor.SinkFunction;
import com.cloudest.mq.stream.processor.SourceProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class TradeAndCreditTaskExample {

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

        StreamTask task = new StreamTask(appId);

        TopicConsumer<TradeMessage> tradeConsumer = new KafkaTopicConsumer<>(topic, appId,
                new JsonSerde<>(TradeMessage.class), createKafkaProperties(brokers));

        String creditTopic = "credit-topic";
        TopicProducer<byte[], CreditMessage> creditProducer = new KafkaTopicProducer<>(creditTopic,
        new ByteArraySerde(), new JsonSerde<>(CreditMessage.class), createKafkaProperties(brokers));

        DataStream<TradeMessage> source = task.createTopicStream(tradeConsumer,
                SourceProcessor.CommitStrategy.AT_EVERY_ONE);

        // credit message
        // filter out the revoke events
        source.filter(new FilterFunction<TradeMessage>() {
            @Override public boolean accept(TradeMessage tuple) {
                return !tuple.isRevokeMessage();
            }
        }).apply(new ApplyFunction<TradeMessage, KeyValue<byte[], CreditMessage>>() {
            // convert from TradeMessage to CreditMessage
            @Override public KeyValue<byte[], CreditMessage> apply(
                    TradeMessage tradeMessage) {
                return new KeyValue(null, new CreditMessage(tradeMessage));
            }
            // sink to credit topic
        }).sinkToTopic(creditProducer);

        // lakala queue message
        source.sink(new SinkFunction<TradeMessage>() {
            @Override public void sink(TradeMessage tuple) {
                System.out.println("Sink message to lakala with tsn: " + tuple.txSn
                + ", and amount: " + tuple.amount
                + ", and event: " + tuple.event);
            }
        });

        task.start();
        // wait 15 seconds to stop the task.
        // only for test
        Thread.sleep(15000);
        task.stop();
    }
}

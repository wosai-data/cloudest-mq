package com.cloudest.mq.tool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.cloudest.mq.util.KafkaConsumerBuilder;

public class MetaTool {

    private void listTopics(KafkaConsumer<?, ?> consumer) {
        Map<String, List<PartitionInfo>> topicPartitions = consumer.listTopics();
        for (String topic : topicPartitions.keySet()) {
            System.out.println(topic);
        }
    }

    private void describeTopic(KafkaConsumer<?, ?> consumer,
                               String topic) {

        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        List<TopicPartition> tps = new ArrayList<>();
        for (PartitionInfo partition: partitions) {
            tps.add(new TopicPartition(topic, partition.partition()));
        }
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(tps);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);

        describeTopicPartitions(topic, partitions, beginningOffsets, endOffsets);

    }

    private void describeTopicPartitions(String topic,
                                         List<PartitionInfo> partitions,
                                         Map<TopicPartition, Long> beginningOffsets,
                                         Map<TopicPartition, Long> endOffsets) {

        System.out.printf("Topic: %s\n", topic);
        for (PartitionInfo partition : partitions) {
            System.out.printf("  Partition: %d\n", partition.partition());
            System.out.printf("      Leader:   %s\n", partition.leader());
            System.out.printf("      Replicas: %s\n", Arrays.asList(partition.replicas()));
            System.out.printf("      Isr:      %s\n", Arrays.asList(partition.inSyncReplicas()));

            TopicPartition tp = new TopicPartition(topic, partition.partition());
            System.out.printf("      Begin:    %d\n", beginningOffsets.get(tp));
            System.out.printf("      End:      %d\n", endOffsets.get(tp));
        }
    }

    public void run(ToolOptions options) throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumerBuilder.builder(options.getBrokers()).build();
        if (options.has("list")) {
            listTopics(consumer);
        }
        if (options.has("describe")) {
            String topic = options.get("describe");
            describeTopic(consumer, topic);
        }
    }
    public static void main(String[] args) throws Exception {

        ToolOptions options = new ToolOptions();
        options.addBrokers();

        OptionGroup group = new OptionGroup();
        group.addOption(Option.builder("L").longOpt("list")
                        .desc("list all topics").build());

        group.addOption(Option.builder("D").hasArg().argName("topic")
                        .longOpt("describe")
                        .desc("describe a topic and its partitions").build());

        group.setRequired(true);
        options.add(group);
        if (!options.parse(args)) {
            return;
        }

        String brokers = options.getBrokers();
        System.out.printf("brokers = %s\n", brokers);

        new MetaTool().run(options);

    }

}

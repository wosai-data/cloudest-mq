package com.cloudest.mq.tool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.cloudest.mq.util.KafkaConsumerBuilder;

public class ConsumerTool {

    private void fetchFrom(KafkaConsumer<byte[], byte[]> consumer, String topic, long starting, int... partitions) {
        List<TopicPartition> tps = new ArrayList<>();
        for(int partition: partitions) {
            tps.add(new TopicPartition(topic, partition));
        }
        consumer.assign(tps);
        if (starting >=0) {
            for (TopicPartition tp: tps) {
                consumer.seek(tp, starting);
            }
        }
        while(true) {
            Iterator<ConsumerRecord<byte[], byte[]>> it =  consumer.poll(2000).iterator();
            while(it.hasNext()) {
                ConsumerRecord<byte[], byte[]> record = it.next();
                int partition = record.partition();
                long offset = record.offset();
                String key = record.key()!=null?new String(record.key()):null;
                String value = record.value()!=null?new String(record.value()):null;
                System.out.printf("partition: %d, offset: %d, key: %s, value: %s\n", partition, offset, key, value);
            }
        }
    }


    public void run(ToolOptions options) throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumerBuilder.builder(options.getBrokers())
                .groupId(options.get("group"))
                .initialOffset("none")
                .build();
        String[] partitions = options.getMulti("partition");
        int[] partitionNumbers = new int[partitions.length];
        for(int i=0; i<partitions.length; ++i) {
            partitionNumbers[i] = Integer.parseInt(partitions[i]);
        }
        long offset = -1;
        if (options.has("offset")) {
            offset = Long.parseLong(options.get("offset"));
        }
        fetchFrom(consumer, options.get("topic"), offset, partitionNumbers);
    }

    public static void main(String[] args) throws Exception {

        ToolOptions options = new ToolOptions();
        options.addBrokers();

        options.add(Option.builder("G").hasArg().argName("group")
                    .longOpt("group")
                    .desc("consumer group of which this consumer is a member").build());

        options.add(Option.builder("T").hasArg().argName("topic")
                    .longOpt("topic")
                    .required()
                    .desc("topic to fetch messages from").build());

        options.add(Option.builder("P").hasArg().argName("partition#")
                    .longOpt("partition")
                    .required()
                    .valueSeparator(',')
                    .desc("partition number(s) to fetch messages from").build());

        options.add(Option.builder("o").hasArg().argName("offset")
                    .longOpt("offset")
                    .desc("offset to read from").build());

        if (!options.parse(args)) {
            return;
        }

        String brokers = options.getBrokers();
        System.out.printf("brokers = %s\n", brokers);

        new ConsumerTool().run(options);

    }

}

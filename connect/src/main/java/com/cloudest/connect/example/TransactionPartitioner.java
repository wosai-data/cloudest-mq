package com.cloudest.connect.example;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class TransactionPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        GenericRecord transaction = (GenericRecord)value;
        Utf8 merchantId = (Utf8)transaction.get("merchant_id");
        if (merchantId == null) {
            return 0;
        } else {
            return Utils.toPositive(Utils.murmur2(merchantId.getBytes())) % numPartitions;
        }
    }

    @Override
    public void close() {
        
    }

}

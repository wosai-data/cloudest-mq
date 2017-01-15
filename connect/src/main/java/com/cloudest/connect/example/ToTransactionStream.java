package com.cloudest.connect.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudest.connect.FilterFunc;
import com.cloudest.connect.KafkaRecordWriter;
import com.cloudest.connect.KafkaTransformer2;
import com.cloudest.connect.Record;
import com.cloudest.connect.RecordWriter;
import com.cloudest.connect.TransformFunc;
import com.cloudest.mq.tool.ToolOptions;

public class ToTransactionStream {
    private static final Logger logger = LoggerFactory.getLogger(ToTransactionStream.class);

    private static boolean isFinalTransactionStatus(int status) {
        return (status / 1000) == 2;
    }

    public void run(ToolOptions options) {
        TransformFunc<Object, Object, String, GenericRecord> extractAfter = new TransformFunc<Object, Object, String, GenericRecord>() {

            @Override
            public Record<String, GenericRecord> apply(Record<Object, Object> input) {
                GenericRecord key = (GenericRecord)input.getKey();
                GenericRecord value = (GenericRecord)input.getValue();
                String id = key.get("id").toString();
                GenericRecord after = (GenericRecord)value.get("after");
                return new Record<String, GenericRecord>(id, after);
            }
        };
        
        FilterFunc<Object, Object> includeFinalUpdates = new FilterFunc<Object, Object>() {

            @Override
            public boolean apply(Object key, Object value) {
                GenericRecord before = (GenericRecord)((GenericRecord)value).get("before");
                GenericRecord after = (GenericRecord)((GenericRecord)value).get("after");
                String op = ((GenericRecord)value).get("op").toString();

                if ("c".equals(op)) {
                    int statusAfter = ((Number)after.get("status")).intValue();
                    if (isFinalTransactionStatus(statusAfter)) {
                        return true;
                    }

                } else if ("u".equals(op)){
                    int statusBefore = ((Number)before.get("status")).intValue();
                    int statusAfter = ((Number)after.get("status")).intValue();
                    if (!isFinalTransactionStatus(statusBefore) && isFinalTransactionStatus(statusAfter)) {
                        return true;
                    }

                }

                return false;
            }
        };
        
        String schemaRegistryUrl = options.get("schema-registry");
        Properties config = new Properties();
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final RecordWriter<String, GenericRecord> writer = options.has("console")?
                                                           new ConsoleRecordWriter<String, GenericRecord>():
                                                           new KafkaRecordWriter<String, GenericRecord>(options.get("brokers"),
                                                                                                        options.get("output-topic"),
                                                                                                        StringSerializer.class,
                                                                                                        KafkaAvroSerializer.class,
                                                                                                        TransactionPartitioner.class,
                                                                                                        config);
        
        final KafkaTransformer2<Object, Object, String, GenericRecord> transformer =
                new KafkaTransformer2<Object, Object, String, GenericRecord>(options.get("group"),
                                                                             options.get("brokers"),
                                                                             options.get("input-topic"),
                                                                             options.getMultiAsInt("partitions"),
                                                                             KafkaAvroDeserializer.class,
                                                                             KafkaAvroDeserializer.class,
                                                                             config,
                                                                             includeFinalUpdates,
                                                                             extractAfter,
                                                                             writer,
                                                                             options.has("reset"));
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            
            @Override
            public void run() {
                logger.info("shutting down transformer ...");
                transformer.shutdown();
                
            }
        }));

        transformer.start();
        
    }

    public static void main(String[] args) throws Exception {

        ToolOptions options = new ToolOptions("ToTransactionStream");

        options.add(Option.builder("B")
                    .hasArg()
                    .argName("brokers")
                    .longOpt("brokers")
                    .required(true)
                    .valueSeparator(',')
                    .desc("kafka brokers where your transformer will read and write.").build());

        options.add(Option.builder("S").hasArg().argName("schema-registry")
                    .longOpt("schema-registry")
                    .required()
                    .desc("schema registry url").build());

        options.add(Option.builder("G").hasArg().argName("group")
                    .longOpt("group")
                    .required()
                    .desc("consumer group of which this transformer instance is a member").build());

        options.add(Option.builder("I").hasArg().argName("input-topic")
                    .longOpt("input-topic")
                    .required()
                    .desc("topic to fetch messages").build());

        options.add(Option.builder("P").hasArgs().argName("partitions")
                    .longOpt("partitions")
                    .required()
                    .valueSeparator(',')
                    .desc("comma seperated partition numbers to fetch messages").build());

        options.add(Option.builder("R")
                    .longOpt("reset")
                    .desc("reset offsets to the beginning of assigned partitions").build());

        OptionGroup group = new OptionGroup();
        
        group.addOption(Option.builder("O").hasArg().argName("output-topic")
                    .longOpt("output-topic")
                    .desc("topic to write transformed messages").build());

        group.addOption(Option.builder("C")
                    .longOpt("console")
                    .desc("write to console").build());

        group.setRequired(true);
        options.add(group);
        
        if (!options.parse(args)) {
            return;
        }

        new ToTransactionStream().run(options);
    }

}

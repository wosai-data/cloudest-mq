package com.cloudest.connect.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
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

    private static Map<Schema, Schema> schemaVariations = new HashMap<Schema, Schema>();

    private static GenericRecord shallowCopyFrom(GenericRecord src, String schemaNamespace, String schemaName) {
        Schema sourceSchema = src.getSchema();
        Schema targetSchema = Schema.createRecord(schemaName, "", schemaNamespace, false);
        List<Schema.Field> sourceFields = sourceSchema.getFields();
        List<Schema.Field> targetFields = new ArrayList<Schema.Field>();
        for (Schema.Field field: sourceFields) {
            targetFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
        }
        targetSchema.setFields(targetFields);

        if (schemaVariations.containsKey(targetSchema)) {
            targetSchema = schemaVariations.get(targetSchema);
        } else {
            schemaVariations.put(targetSchema, targetSchema);
        }

        GenericRecord target = new GenericData.Record(targetSchema);
        for (Schema.Field field: sourceFields) {
            target.put(field.name(), src.get(field.name()));
        }
        return target;
    }

    public void run(ToolOptions options) {
        final String schemaNamespace = options.get("schema-namespace");
        final String schemaName = options.get("schema-name");

        TransformFunc<Object, Object, String, GenericRecord> extractAfter = new TransformFunc<Object, Object, String, GenericRecord>() {

            @Override
            public Record<String, GenericRecord> apply(Record<Object, Object> input) {
                GenericRecord key = (GenericRecord)input.getKey();
                GenericRecord value = (GenericRecord)input.getValue();
                String id = key.get("id").toString();
                GenericRecord after = (GenericRecord)value.get("after");
                return new Record<String, GenericRecord>(id, shallowCopyFrom(after, schemaNamespace, schemaName));
            }
        };
        
        FilterFunc<Object, Object> includeFinalUpdates = new FilterFunc<Object, Object>() {

            @Override
            public boolean apply(Object key, Object value) {
                GenericRecord before = (GenericRecord)((GenericRecord)value).get("before");
                GenericRecord after = (GenericRecord)((GenericRecord)value).get("after");
                String op = ((GenericRecord)value).get("op").toString();

                if ("c".equals(op)) {
                    Object paidAmount = after.get("paid_amount");
                    if (paidAmount != null) {
                        return true;
                    }

                } else if ("u".equals(op)){
                    Object paidAmountBefore = before.get("paid_amount");
                    Object paidAmountAfter = after.get("paid_amount");

                    if (paidAmountBefore == null && paidAmountAfter != null) {
                        return true;
                    }

                }

                return false;
            }
        };
        
        String schemaRegistryUrl = options.get("schema-registry");
        Properties config = new Properties();
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);


        String[] inputTopics = options.getMulti("input-topic");
        final List<KafkaTransformer2<Object, Object, String, GenericRecord>> transformers = new ArrayList<>();
        for (String inputTopic: inputTopics) {
            String[] fields = inputTopic.split(":");
            if (fields.length < 2) {
                logger.error("bad input-topic format: {}", inputTopic);
                return;
            }
            String topic = fields[0];
            int[] partitions = new int[fields.length-1];
            for (int i=1; i<fields.length; ++i) {
                partitions[i-1] = Integer.parseInt(fields[i]);
            }
            
            final RecordWriter<String, GenericRecord> writer = options.has("console")?
                                                               new ConsoleRecordWriter<String, GenericRecord>():
                                                               new KafkaRecordWriter<String, GenericRecord>(options.get("brokers"),
                                                                                                            options.get("output-topic"),
                                                                                                            KafkaAvroSerializer.class,
                                                                                                            KafkaAvroSerializer.class,
                                                                                                            TransactionPartitioner.class,
                                                                                                            config);

            transformers.add(new KafkaTransformer2<Object, Object, String, GenericRecord>(options.get("group"),
                                                                                          options.get("brokers"),
                                                                                          topic,
                                                                                          partitions,
                                                                                          KafkaAvroDeserializer.class,
                                                                                          KafkaAvroDeserializer.class,
                                                                                          config,
                                                                                          includeFinalUpdates,
                                                                                          extractAfter,
                                                                                          writer,
                                                                                          options.has("reset")));
            
            logger.info("transforming {}", inputTopic);

        }

        for (KafkaTransformer2<Object, Object, String, GenericRecord> transformer: transformers) {
            transformer.start();
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            
            @Override
            public void run() {
                logger.info("shutting down transformer ...");

                for (KafkaTransformer2<Object, Object, String, GenericRecord> transformer: transformers) {
                    transformer.shutdown();
                }
                
                for (KafkaTransformer2<Object, Object, String, GenericRecord> transformer: transformers) {
                    transformer.processorWait();
                }
                
            }
        }));

        
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
                    .valueSeparator(',')
                    .desc("specify input-topics in this format <topic_name>:<partition>[:<partition> ...][,<topic_name>:<partition>[<partition> ...]...]").build());

        /*
        options.add(Option.builder("P").hasArgs().argName("partitions")
                    .longOpt("partitions")
                    .required()
                    .valueSeparator(',')
                    .desc("comma seperated partition numbers to fetch messages").build());
         */

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
        
        options.add(Option.builder("N").hasArg().argName("schema-name")
                    .longOpt("schema-name")
                    .required()
                    .desc("schema name of the output record").build());

        options.add(Option.builder("P").hasArg().argName("schema-namespace")
                    .longOpt("schema-namespace")
                    .required()
                    .desc("schema namespace of the output record").build());


        if (!options.parse(args)) {
            return;
        }

        new ToTransactionStream().run(options);
    }

}

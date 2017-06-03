package com.cloudest.connect;

import com.cloudest.connect.example.ConsoleRecordWriter;
import com.cloudest.connect.example.TransactionPartitioner;
import com.cloudest.mq.tool.ToolOptions;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by terry on 2017/6/2.
 */
public abstract class AbstractToStream {

    private static final Logger logger = LoggerFactory.getLogger(AbstractToStream.class);

    // schema cache
    private Map<Schema, Schema> schemaVariations = new HashMap<>();

    /**
     * 浅拷贝，只第一层field
     *
     * @param src
     * @param schemaNamespace
     * @param schemaName
     * @return
     */
    protected synchronized GenericRecord shallowCopyFrom(GenericRecord src, String schemaNamespace, String schemaName) {
        Schema sourceSchema = src.getSchema();
        Schema targetSchema = Schema.createRecord(schemaName, "", schemaNamespace, false);
        List<Schema.Field> sourceFields = sourceSchema.getFields();
        List<Schema.Field> targetFields = new ArrayList<>();
        for (Schema.Field field : sourceFields) {
            targetFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
        }
        targetSchema.setFields(targetFields);
        if (schemaVariations.containsKey(targetSchema)) {
            targetSchema = schemaVariations.get(targetSchema);
        } else {
            schemaVariations.put(targetSchema, targetSchema);
        }
        GenericRecord target = new GenericData.Record(targetSchema);
        for (Schema.Field field : sourceFields) {
            target.put(field.name(), src.get(field.name()));
        }
        return target;
    }


    /**
     * 工厂方法，创建schema转换函数
     * <p>
     * hook，可以被子类override
     *
     * @param schemaNamespace
     * @param schemaName
     * @return func implement TransformFunc
     */
    protected TransformFunc createTransformFunc(final String schemaNamespace, final String schemaName) {
        return new TransformFunc<Object, Object, String, GenericRecord>() {
            @Override
            public Record<String, GenericRecord> apply(Record<Object, Object> input) {
                GenericRecord key = (GenericRecord) input.getKey();
                GenericRecord value = (GenericRecord) input.getValue();
                String id = key.get("id").toString();
                GenericRecord after = (GenericRecord) value.get("after");
                return new Record<>(id, shallowCopyFrom(after, schemaNamespace, schemaName));
            }
        };
    }


    /**
     * 创建配置对象
     *
     * @param options
     * @param configKeyFieldMapping { config.key => toolOptions.key } example: {KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "schema-registry"};
     * @return
     */
    private Properties createConfig(ToolOptions options, Map<String, String> configKeyFieldMapping) {
        Properties config = new Properties();
        if (configKeyFieldMapping != null && configKeyFieldMapping.size() > 0) {
            for (String key : configKeyFieldMapping.keySet()) {
                config.put(key, options.get(configKeyFieldMapping.get(key)));
            }
        }
        return config;
    }

    protected RecordWriter createRecordWriter(ToolOptions options, Properties config) {
        return options.has("console") ?
                new ConsoleRecordWriter<String, GenericRecord>() :
                new KafkaRecordWriter<String, GenericRecord>(options.get("brokers"),
                        options.get("output-topic"),
                        KafkaAvroSerializer.class,
                        KafkaAvroSerializer.class,
                        TransactionPartitioner.class,
                        config);
    }


    public void run(ToolOptions options) {

        List<KafkaTransformer2<Object, Object, String, GenericRecord>> transformer2List = createTransformList(options);
        if (transformer2List != null && transformer2List.size() > 0)
            bootAllTransformers(transformer2List);

    }

    private void bootAllTransformers(final List<KafkaTransformer2<Object, Object, String, GenericRecord>> transformer2List) {

        for (KafkaTransformer2<Object, Object, String, GenericRecord> transformer : transformer2List) {
            transformer.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("shutting down transformer ...");
                for (KafkaTransformer2<Object, Object, String, GenericRecord> transformer : transformer2List) {
                    transformer.shutdown();
                }
                for (KafkaTransformer2<Object, Object, String, GenericRecord> transformer : transformer2List) {
                    transformer.processorWait();
                }
            }
        }));

    }


    protected List<KafkaTransformer2<Object, Object, String, GenericRecord>> createTransformList(ToolOptions options) {

        Properties config = createConfig(
                options,
                new HashMap<String, String>() {
                    {
                        put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "schema-registry");
                    }
                });

        FilterFunc filterFunc = createFilterFunc();

        final String schemaNamespace = options.get("schema-namespace");
        final String schemaName = options.get("schema-name");
        TransformFunc transformFunc = createTransformFunc(schemaNamespace, schemaName);

        RecordWriter recordWriter = createRecordWriter(options, config);

        String[] inputTopics = options.getMulti("input-topic");
        final List<KafkaTransformer2<Object, Object, String, GenericRecord>> transformers = new ArrayList<>();
        for (String inputTopic : inputTopics) {
            String[] fields = inputTopic.split(":");
            if (fields.length < 2) {
                logger.error("bad input-topic format: {}", inputTopic);
                return null;
            }
            String topic = fields[0];
            int[] partitions = new int[fields.length - 1];
            for (int i = 1; i < fields.length; ++i) {
                partitions[i - 1] = Integer.parseInt(fields[i]);
            }

            transformers.add(new KafkaTransformer2<>(options.get("group"),
                    options.get("brokers"),
                    topic,
                    partitions,
                    KafkaAvroDeserializer.class,
                    KafkaAvroDeserializer.class,
                    config,
                    filterFunc,
                    transformFunc,
                    recordWriter,
                    options.has("reset")));

            logger.info("transforming {}", inputTopic);

        }
        return transformers;
    }

    /**
     * 创建过滤函数
     *
     * @return
     */
    protected abstract FilterFunc createFilterFunc();

    /**
     * cli option build
     *
     * @param toolsName
     * @return
     */
    public static ToolOptions buildToolOptions(String toolsName) {

        ToolOptions options = new ToolOptions(toolsName);

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
        return options;
    }


}

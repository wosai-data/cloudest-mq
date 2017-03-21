package com.cloudest.connect.example;

import java.util.Properties;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudest.connect.KafkaRecordWriter;
import com.cloudest.connect.KafkaTransformer2;
import com.cloudest.connect.Record;
import com.cloudest.connect.RecordWriter;
import com.cloudest.connect.TransformFunc;
import com.cloudest.mq.tool.ToolOptions;

public class ToUpper {
    private static final Logger logger = LoggerFactory.getLogger(ToUpper.class);

    public void run(ToolOptions options) {
        TransformFunc<String, String, String, String> toUpper = new TransformFunc<String, String, String, String>() {

            @Override
            public Record<String, String> apply(Record<String, String> input) {
                String key = input.getKey();
                if (key != null) {
                    key = key.toUpperCase();
                }
                String value = input.getValue();
                if (value != null) {
                    value = value.toUpperCase();
                }
                return new Record<String, String>(key, value);
            }
        };

        final RecordWriter<String, String> writer = options.has("console")?
                                                    new ConsoleRecordWriter<String, String>():
                                                    new KafkaRecordWriter<>(options.get("brokers"),
                                                                            options.get("output-topic"),
                                                                            StringSerializer.class,
                                                                            StringSerializer.class,
                                                                            DefaultPartitioner.class,
                                                                            new Properties()); 
        
        final KafkaTransformer2<String, String, String, String> transformer = new KafkaTransformer2<String, String, String, String>(options.get("group"),
                options.get("brokers"), options.get("input-topic"), options.getMultiAsInt("partitions"),
                StringDeserializer.class, StringDeserializer.class, new Properties(),
                null, toUpper, writer, options.has("reset"));
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            
            @Override
            public void run() {
                logger.info("shutting down transformer ...");
                transformer.shutdownAndWait();
                
            }
        }));

        transformer.start();
        
    }

    public static void main(String[] args) throws Exception {

        ToolOptions options = new ToolOptions("ToUpper");

        options.add(Option.builder("B")
                    .hasArg()
                    .argName("brokers")
                    .longOpt("brokers")
                    .required(true)
                    .valueSeparator(',')
                    .desc("kafka brokers where your transformer will read and write.").build());

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

        new ToUpper().run(options);
    }

}

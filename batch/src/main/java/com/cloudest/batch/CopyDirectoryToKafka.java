package com.cloudest.batch;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Properties;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudest.connect.KafkaRecordWriter;
import com.cloudest.connect.Record;
import com.cloudest.connect.RecordWriter;
import com.cloudest.connect.RecordWriterCallback;
import com.cloudest.mq.tool.ToolOptions;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class CopyDirectoryToKafka {
    private static final Logger logger = LoggerFactory.getLogger(CopyDirectoryToKafka.class);

    private static final String OPT_BROKERS = "brokers";
    private static final String OPT_SCHEMA_REGISTRY = "schema-registry";
    private static final String OPT_OUTPUT_TOPIC = "output-topic";
    private static final String OPT_KEY_COLUMN = "key-column";
    private static final String OPT_SCHEMA = "schema";
    private static final String OPT_INPUT_DIR = "input-dir";
    
    public void run(ToolOptions options) throws Exception {
        String schemaRegistryUrl = options.get(OPT_SCHEMA_REGISTRY);
        String brokers = options.get(OPT_BROKERS);
        String topic = options.get(OPT_OUTPUT_TOPIC);
        String keyColumn = options.get(OPT_KEY_COLUMN);

        Properties config = new Properties();
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        RecordWriter<String, GenericRecord> writer = new KafkaRecordWriter<String, GenericRecord>(brokers,
                                                                                                  topic,
                                                                                                  KafkaAvroSerializer.class,
                                                                                                  KafkaAvroSerializer.class,
                                                                                                  DefaultPartitioner.class,
                                                                                                  config);
        URI uri = new URI(options.get(OPT_INPUT_DIR));
        
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(uri, configuration);

        FileStatus[] files = fs.listStatus(new Path(uri));
        for (FileStatus file: files) {
            if (file.getLen() > 0) {
                logger.info("saving file into kafka {}", file);
                FsInput input = new FsInput(file.getPath(), configuration);
                DatumReader<GenericRecord> reader = new GenericDatumReader<>();
                FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);


                while(fileReader.hasNext()) {
                    GenericRecord record = fileReader.next();
                    String key = (String)record.get(keyColumn);
                    
                    writer.write(new Record<String, GenericRecord>(key, record), new RecordWriterCallback() {
                        @Override
                        public void onSuccess() {
                            
                        }
                        
                        @Override
                        public void onError(Exception ex) {
                            
                        }
                    });
                }
                fileReader.close();
            }
        }
        writer.close();
    }

    public static void main(String[] args) throws Exception {

        ToolOptions options = new ToolOptions("ToTransactionStream");

        options.add(Option.builder("B")
                    .hasArg()
                    .argName(OPT_BROKERS)
                    .longOpt(OPT_BROKERS)
                    .required(true)
                    .valueSeparator(',')
                    .desc("kafka brokers where to sink your data").build());

        options.add(Option.builder("S").hasArg().argName(OPT_SCHEMA_REGISTRY)
                    .longOpt(OPT_SCHEMA_REGISTRY)
                    .required()
                    .desc("schema registry url where to register your topic schema").build());

        options.add(Option.builder("O").hasArg().argName(OPT_OUTPUT_TOPIC)
                    .longOpt(OPT_OUTPUT_TOPIC)
                    .required()
                    .desc("output topic name").build());

        options.add(Option.builder("K").hasArg().argName(OPT_KEY_COLUMN)
                    .longOpt(OPT_KEY_COLUMN)
                    .required()
                    .desc("key column name").build());

        options.add(Option.builder("M").hasArg().argName(OPT_SCHEMA)
                    .longOpt(OPT_SCHEMA)
                    .desc("the schema must be compatible with your data.").build());

        options.add(Option.builder("I").hasArg().argName(OPT_INPUT_DIR)
                    .longOpt(OPT_INPUT_DIR)
                    .required()
                    .desc("the input directory").build());

        
        if (!options.parse(args)) {
            return;
        }
        
        new CopyDirectoryToKafka().run(options);

    }

}

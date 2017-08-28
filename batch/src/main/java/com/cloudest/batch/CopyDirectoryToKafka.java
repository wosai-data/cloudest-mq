package com.cloudest.batch;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
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
    private static final String OPT_ADD_COLUMN = "add-column";
    private static final String OPT_COLUMN_VALUE = "column-value";
    
    private static Map<Schema, Schema> schemaVariations = new HashMap<Schema, Schema>();

    private static Schema copySchema(Schema sourceSchema, String extraStringColumn) {
        String namespace = sourceSchema.getNamespace();
        String schemaName = sourceSchema.getName() + "Copy";

        Schema targetSchema = Schema.createRecord(schemaName, "", namespace, false);
        List<Schema.Field> sourceFields = sourceSchema.getFields();
        List<Schema.Field> targetFields = new ArrayList<Schema.Field>();
        for (Schema.Field field: sourceFields) {
            targetFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
        }
        if (extraStringColumn != null) {
            targetFields.add(new Schema.Field(extraStringColumn, Schema.create(Schema.Type.STRING), "programmatically added column", null, Schema.Field.Order.ASCENDING));
        }
        targetSchema.setFields(targetFields);

        if (schemaVariations.containsKey(targetSchema)) {
            targetSchema = schemaVariations.get(targetSchema);
        } else {
            schemaVariations.put(targetSchema, targetSchema);
        }

        return targetSchema;
    }
    

    public void run(ToolOptions options) throws Exception {
        String schemaRegistryUrl = options.get(OPT_SCHEMA_REGISTRY);
        String brokers = options.get(OPT_BROKERS);
        String topic = options.get(OPT_OUTPUT_TOPIC);
        String keyColumn = options.get(OPT_KEY_COLUMN);
        String addColumn = options.get(OPT_ADD_COLUMN);
        String columnValue = options.get(OPT_COLUMN_VALUE);

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

                Schema sourceSchema = fileReader.getSchema();
                Schema targetSchema = copySchema(sourceSchema, addColumn);
                
                int count = 0;
                while(fileReader.hasNext()) {
                    GenericRecord record = fileReader.next();
                    Object keyObject = record.get(keyColumn);
                    if (keyObject == null) {
                        throw new IOException(String.format("key column is null. column name %s record %s", keyColumn, record));
                    }
                    if ( !(keyObject instanceof Utf8) ) {
                        throw new IOException(String.format("key column is not of type utf8. column name %s record %s", keyColumn, record));
                    }
                    
                    String key = ((Utf8)keyObject).toString();
                    
                    GenericRecord target = new GenericData.Record(targetSchema);
                    for (Schema.Field field: sourceSchema.getFields()) {
                        target.put(field.name(), record.get(field.name()));
                    }
                    if (addColumn != null) {
                        target.put(addColumn, columnValue);
                    }

                    writer.write(new Record<String, GenericRecord>(key, target), new RecordWriterCallback() {
                        @Override
                        public void onSuccess() {
//                            logger.info("successfully sent record to kafka topic.");
                        }
                        
                        @Override
                        public void onError(Exception ex) {
                            logger.warn("failed to send record to kafka topic.", ex);
                        }
                    });
                    ++count;
                }
                fileReader.close();
                
                logger.info("moved {} records from file into kafka producer for topic {}", count, topic);

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

        options.add(Option.builder("A").hasArg().argName(OPT_ADD_COLUMN)
                    .longOpt(OPT_ADD_COLUMN)
                    .required(false)
                    .desc("add a new column named the given value").build());

        options.add(Option.builder("V").hasArg().argName(OPT_COLUMN_VALUE)
                    .longOpt(OPT_COLUMN_VALUE)
                    .required()
                    .desc("set the value of the new column to the given value").build());
        
        if (!options.parse(args)) {
            return;
        }
        
        new CopyDirectoryToKafka().run(options);

    }

}

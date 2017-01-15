package com.cloudest.connect.example;

import com.cloudest.connect.Record;
import com.cloudest.connect.RecordWriter;
import com.cloudest.connect.RecordWriterCallback;

public class ConsoleRecordWriter<K, V> implements RecordWriter<K, V> {

    @Override
    public void write(Record<K, V> record, RecordWriterCallback callback) {
        System.out.printf("%s\t%s\n", record.getKey(), record.getValue() );

        callback.onSuccess();
    }

    @Override
    public void close() {
        System.out.flush();
    }

}

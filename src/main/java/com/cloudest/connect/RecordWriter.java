package com.cloudest.connect;

public interface RecordWriter<K,V> {
    void write(Record<K,V> record, RecordWriterCallback callback);

    void close();
}

package com.cloudest.connect;

public interface RecordWriterCallback {
    void onSuccess();
    void onError(Exception ex);

}

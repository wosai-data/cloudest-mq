package com.cloudest.mq.stream;

public class StreamTaskException extends RuntimeException {

    public StreamTaskException(String message) {
        super(message == null ? "" : ": " + message);
    }
}

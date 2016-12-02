package com.cloudest.mq.common;

/**
 * The base class of all other cloudest mq exceptions
 */
public class MessagingException extends RuntimeException {

    private final static long serialVersionUID = 1L;

    public MessagingException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessagingException(String message) {
        super(message);
    }

    public MessagingException(Throwable cause) {
        super(cause);
    }

    public MessagingException() {
        super();
    }

}


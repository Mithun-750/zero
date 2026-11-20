package com.zero.consumer.exception;

/** Base class for all consumer-side errors. */
public class ConsumerException extends RuntimeException {

    public ConsumerException(String message) {
        super(message);
    }

    public ConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}

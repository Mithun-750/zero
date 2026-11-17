package com.zero.consumer;

/**
 * DLQ write failed — the source offset MUST NOT be committed;
 * the record will be redelivered.
 */
public class DlqPublishException extends ConsumerException {
    public DlqPublishException(String message) { super(message); }
    public DlqPublishException(String message, Throwable cause) { super(message, cause); }
}

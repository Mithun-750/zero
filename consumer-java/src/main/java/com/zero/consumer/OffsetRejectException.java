package com.zero.consumer;

/**
 * Outcome was NOT persisted durably.
 * Callers MUST abstain from committing the Kafka offset.
 * The record will be redelivered (at-least-once delivery).
 */
public class OffsetRejectException extends ConsumerException {
    public OffsetRejectException(String message) { super(message); }
    public OffsetRejectException(String message, Throwable cause) { super(message, cause); }
}

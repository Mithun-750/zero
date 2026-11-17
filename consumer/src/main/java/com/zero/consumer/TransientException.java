package com.zero.consumer;

/**
 * Retryable infra failure (DB connection, broker hiccup).
 * Pipeline retries up to consumerMaxRetries times with backoff;
 * on exhaustion the record is routed to DLQ as TRANSIENT_EXHAUSTED.
 * failure_class = TRANSIENT_EXHAUSTED (after retries)
 */
public class TransientException extends ConsumerException {
    public static final String FAILURE_CLASS = "TRANSIENT_EXHAUSTED";

    public TransientException(String message) { super(message); }
    public TransientException(String message, Throwable cause) { super(message, cause); }
}

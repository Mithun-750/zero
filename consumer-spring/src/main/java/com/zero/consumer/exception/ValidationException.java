package com.zero.consumer.exception;

/**
 * Permanent error — event cannot be applied as-is.
 * Validation failures: unparseable, invalid envelope, unsupported version.
 * failure_class = VALIDATION
 */
public class ValidationException extends ConsumerException {

    public static final String FAILURE_CLASS = "VALIDATION";

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getFailureClass() {
        return FAILURE_CLASS;
    }
}

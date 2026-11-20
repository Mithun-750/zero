package com.zero.consumer.exception;

/**
 * Permanent error — event is parseable but violates domain business rules.
 * E.g. amount <= 0, bad currency.
 * failure_class = BUSINESS_RULE
 */
public class BusinessRuleException extends ConsumerException {

    public static final String FAILURE_CLASS = "BUSINESS_RULE";

    public BusinessRuleException(String message) {
        super(message);
    }

    public BusinessRuleException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getFailureClass() {
        return FAILURE_CLASS;
    }
}

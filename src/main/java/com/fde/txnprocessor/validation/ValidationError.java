package com.fde.txnprocessor.validation;

/**
 * A field-level validation failure. Carries the field name and an error code only, never the
 * offending value, so rejection reasons are safe to log and to return to callers.
 */
public record ValidationError(String field, String code) {

    @Override
    public String toString() {
        return field + ":" + code;
    }
}

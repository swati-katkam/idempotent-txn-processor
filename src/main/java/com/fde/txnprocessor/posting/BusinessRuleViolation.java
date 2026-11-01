package com.fde.txnprocessor.posting;

/** A deterministic rejection by business rules. Retrying would give the same answer. */
public class BusinessRuleViolation extends RuntimeException {

    private final String code;

    public BusinessRuleViolation(String code) {
        super(code);
        this.code = code;
    }

    public String code() {
        return code;
    }
}

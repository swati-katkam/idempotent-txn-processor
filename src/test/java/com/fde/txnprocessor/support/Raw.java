package com.fde.txnprocessor.support;

import com.fde.txnprocessor.domain.RawTransactionRequest;

/** Fluent builder for raw requests in tests. Defaults to a valid USD credit. */
public final class Raw {

    private String sourceRef = "test";
    private String transactionId;
    private String requestId;
    private String accountId = "ACC-1";
    private String type = "CREDIT";
    private String amount = "100";
    private String currency = "USD";
    private String sequence;
    private String eventTime;
    private String originalTransactionId;

    private Raw(String transactionId, String requestId, long sequence) {
        this.transactionId = transactionId;
        this.requestId = requestId;
        this.sequence = Long.toString(sequence);
        this.sourceRef = "test:" + requestId;
    }

    public static Raw txn(String transactionId, String requestId, long sequence) {
        return new Raw(transactionId, requestId, sequence);
    }

    public Raw account(String accountId) {
        this.accountId = accountId;
        return this;
    }

    public Raw credit(String amount) {
        this.type = "CREDIT";
        this.amount = amount;
        return this;
    }

    public Raw debit(String amount) {
        this.type = "DEBIT";
        this.amount = amount;
        return this;
    }

    public Raw reversal(String amount, String originalTransactionId) {
        this.type = "REVERSAL";
        this.amount = amount;
        this.originalTransactionId = originalTransactionId;
        return this;
    }

    public Raw type(String type) {
        this.type = type;
        return this;
    }

    public Raw amount(String amount) {
        this.amount = amount;
        return this;
    }

    public Raw currency(String currency) {
        this.currency = currency;
        return this;
    }

    public Raw sequence(String sequence) {
        this.sequence = sequence;
        return this;
    }

    public Raw transactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public Raw requestId(String requestId) {
        this.requestId = requestId;
        return this;
    }

    public Raw eventTime(String eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    public Raw original(String originalTransactionId) {
        this.originalTransactionId = originalTransactionId;
        return this;
    }

    public RawTransactionRequest build() {
        return new RawTransactionRequest(sourceRef, transactionId, requestId, accountId, type, amount, currency,
                sequence, eventTime, originalTransactionId, null);
    }
}

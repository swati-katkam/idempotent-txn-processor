package com.fde.txnprocessor.domain;

public class IllegalStatusTransitionException extends IllegalStateException {

    public IllegalStatusTransitionException(String transactionId, TransactionStatus from, TransactionStatus to) {
        super("Illegal status transition for " + transactionId + ": " + from + " -> " + to);
    }
}

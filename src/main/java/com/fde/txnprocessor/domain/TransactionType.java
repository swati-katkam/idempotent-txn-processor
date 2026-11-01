package com.fde.txnprocessor.domain;

public enum TransactionType {
    CREDIT,
    DEBIT,
    /** Reverses a previously posted CREDIT or DEBIT identified by originalTransactionId. */
    REVERSAL
}

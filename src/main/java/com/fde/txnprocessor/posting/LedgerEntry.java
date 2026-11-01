package com.fde.txnprocessor.posting;

import com.fde.txnprocessor.domain.TransactionRequest;
import com.fde.txnprocessor.domain.TransactionType;

import java.math.BigDecimal;
import java.util.Objects;

/** The side effect the service exists to produce: one posting per business transaction. */
public record LedgerEntry(String transactionId, String accountId, TransactionType type, BigDecimal amount,
                          String currency, String originalTransactionId) {

    public static LedgerEntry from(TransactionRequest request) {
        return new LedgerEntry(request.transactionId(), request.accountId(), request.type(), request.amount(),
                request.currency(), request.originalTransactionId());
    }

    boolean samePostingAs(LedgerEntry other) {
        return transactionId.equals(other.transactionId)
                && accountId.equals(other.accountId)
                && type == other.type
                && amount.compareTo(other.amount) == 0
                && currency.equals(other.currency)
                && Objects.equals(originalTransactionId, other.originalTransactionId);
    }
}

package com.fde.txnprocessor.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Lifecycle of a business transaction (keyed by transactionId).
 *
 * <pre>
 * RECEIVED --+--> PROCESSING --+--> PROCESSED
 *            |       ^         +--> RETRY_PENDING --> PROCESSING   (bounded by RetryPolicy)
 *            |       |         +--> FAILED --> RETRY_PENDING      (operator replay only)
 *            +--> PENDING      (waiting for an earlier sequence number)
 *            +--> REJECTED     (sequence slot conflict)
 * </pre>
 *
 * DUPLICATE is a request-level outcome: the duplicate request is reported as DUPLICATE while the
 * transaction it duplicates keeps its own status. Requests that fail validation are REJECTED
 * before any transaction record exists.
 */
public enum TransactionStatus {
    RECEIVED,
    PENDING,
    PROCESSING,
    RETRY_PENDING,
    PROCESSED,
    DUPLICATE,
    REJECTED,
    FAILED;

    private static final Map<TransactionStatus, Set<TransactionStatus>> ALLOWED = new EnumMap<>(TransactionStatus.class);

    static {
        ALLOWED.put(RECEIVED, EnumSet.of(PENDING, PROCESSING, REJECTED));
        ALLOWED.put(PENDING, EnumSet.of(PROCESSING));
        ALLOWED.put(PROCESSING, EnumSet.of(PROCESSED, RETRY_PENDING, FAILED));
        ALLOWED.put(RETRY_PENDING, EnumSet.of(PROCESSING));
        ALLOWED.put(FAILED, EnumSet.of(RETRY_PENDING));
        ALLOWED.put(PROCESSED, EnumSet.noneOf(TransactionStatus.class));
        ALLOWED.put(DUPLICATE, EnumSet.noneOf(TransactionStatus.class));
        ALLOWED.put(REJECTED, EnumSet.noneOf(TransactionStatus.class));
    }

    public boolean canTransitionTo(TransactionStatus next) {
        return ALLOWED.get(this).contains(next);
    }

    /** Terminal for the automated pipeline. FAILED can still be replayed by an operator. */
    public boolean isTerminal() {
        return this == PROCESSED || this == DUPLICATE || this == REJECTED || this == FAILED;
    }
}

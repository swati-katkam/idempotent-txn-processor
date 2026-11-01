package com.fde.txnprocessor.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Immutable snapshot of a transaction's processing state. Every change goes through
 * {@link #transitionTo}, which enforces the lifecycle in {@link TransactionStatus} and appends to the
 * audit trail, so a store can never persist an illegal state.
 */
public final class TransactionRecord {

    private final TransactionRequest request;
    private final String fingerprint;
    private final TransactionStatus status;
    private final int attempts;
    private final String reason;
    private final Instant updatedAt;
    private final List<StatusChange> history;

    private TransactionRecord(TransactionRequest request, String fingerprint, TransactionStatus status, int attempts,
                              String reason, Instant updatedAt, List<StatusChange> history) {
        this.request = request;
        this.fingerprint = fingerprint;
        this.status = status;
        this.attempts = attempts;
        this.reason = reason;
        this.updatedAt = updatedAt;
        this.history = history;
    }

    public static TransactionRecord received(TransactionRequest request, String fingerprint, Instant now) {
        return new TransactionRecord(request, fingerprint, TransactionStatus.RECEIVED, 0, null, now,
                List.of(new StatusChange(null, TransactionStatus.RECEIVED, null, now)));
    }

    public TransactionRecord transitionTo(TransactionStatus next, String reason, Instant now) {
        if (!status.canTransitionTo(next)) {
            throw new IllegalStatusTransitionException(transactionId(), status, next);
        }
        int nextAttempts = switch (next) {
            case PROCESSING -> attempts + 1;
            // an operator replay of a FAILED transaction grants a fresh retry budget
            case RETRY_PENDING -> status == TransactionStatus.FAILED ? 0 : attempts;
            default -> attempts;
        };
        List<StatusChange> nextHistory = new ArrayList<>(history);
        nextHistory.add(new StatusChange(status, next, reason, now));
        return new TransactionRecord(request, fingerprint, next, nextAttempts, reason, now, List.copyOf(nextHistory));
    }

    public String transactionId() {
        return request.transactionId();
    }

    public TransactionRequest request() {
        return request;
    }

    public String fingerprint() {
        return fingerprint;
    }

    public TransactionStatus status() {
        return status;
    }

    /** Number of times processing has been attempted (entries into PROCESSING). */
    public int attempts() {
        return attempts;
    }

    /** Reason attached to the latest transition, e.g. a failure code. */
    public String reason() {
        return reason;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    public List<StatusChange> history() {
        return history;
    }

    /** Compact lifecycle string such as {@code RECEIVED>PENDING>PROCESSING>PROCESSED}. */
    public String lifecycle() {
        return history.stream().map(c -> c.to().name()).collect(Collectors.joining(">"));
    }
}

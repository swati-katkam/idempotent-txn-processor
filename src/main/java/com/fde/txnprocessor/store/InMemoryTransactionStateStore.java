package com.fde.txnprocessor.store;

import com.fde.txnprocessor.domain.TransactionRecord;
import com.fde.txnprocessor.domain.TransactionRequest;
import com.fde.txnprocessor.domain.TransactionStatus;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory store. {@link #claim} is synchronized so the check-then-insert on both
 * dedup keys is atomic; {@link #transition} relies on {@code computeIfPresent}, which is atomic per
 * key. State is lost on restart, which is acceptable for this exercise (see DESIGN.md).
 */
public final class InMemoryTransactionStateStore implements TransactionStateStore {

    private final Map<String, TransactionRecord> byTransactionId = new ConcurrentHashMap<>();
    private final Map<String, String> transactionIdByRequestId = new ConcurrentHashMap<>();

    @Override
    public synchronized ClaimResult claim(TransactionRequest request, String fingerprint, Instant now) {
        TransactionRecord existing = byTransactionId.get(request.transactionId());
        String knownTransactionId = transactionIdByRequestId.get(request.requestId());

        if (knownTransactionId != null) {
            boolean sameTransaction = knownTransactionId.equals(request.transactionId())
                    && existing != null && existing.fingerprint().equals(fingerprint);
            return sameTransaction
                    ? new ClaimResult(ClaimResult.Outcome.DUPLICATE_REQUEST, existing)
                    : new ClaimResult(ClaimResult.Outcome.CONFLICT, byTransactionId.get(knownTransactionId));
        }
        if (existing != null) {
            if (!existing.fingerprint().equals(fingerprint)) {
                return new ClaimResult(ClaimResult.Outcome.CONFLICT, existing);
            }
            transactionIdByRequestId.put(request.requestId(), request.transactionId());
            return new ClaimResult(ClaimResult.Outcome.DUPLICATE_TRANSACTION, existing);
        }

        TransactionRecord created = TransactionRecord.received(request, fingerprint, now);
        byTransactionId.put(request.transactionId(), created);
        transactionIdByRequestId.put(request.requestId(), request.transactionId());
        return new ClaimResult(ClaimResult.Outcome.NEW, created);
    }

    @Override
    public TransactionRecord transition(String transactionId, TransactionStatus to, String reason, Instant now) {
        TransactionRecord updated = byTransactionId.computeIfPresent(transactionId,
                (id, current) -> current.transitionTo(to, reason, now));
        if (updated == null) {
            throw new NoSuchElementException("Unknown transaction " + transactionId);
        }
        return updated;
    }

    @Override
    public Optional<TransactionRecord> find(String transactionId) {
        return Optional.ofNullable(byTransactionId.get(transactionId));
    }

    @Override
    public List<TransactionRecord> findAll() {
        return byTransactionId.values().stream()
                .sorted(Comparator.comparingLong((TransactionRecord r) -> r.request().sequenceNumber())
                        .thenComparing(TransactionRecord::transactionId))
                .toList();
    }
}

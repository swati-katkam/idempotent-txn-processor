package com.fde.txnprocessor.store;

import com.fde.txnprocessor.domain.TransactionRecord;
import com.fde.txnprocessor.domain.TransactionRequest;
import com.fde.txnprocessor.domain.TransactionStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Port for the idempotency and transaction-state store.
 *
 * <p>A database-backed implementation would map {@link #claim} to an INSERT guarded by unique
 * constraints on {@code transaction_id} and {@code request_id}, and {@link #transition} to an
 * optimistic {@code UPDATE ... WHERE status = :expected AND version = :version}.
 */
public interface TransactionStateStore {

    /**
     * Atomically checks the dedup keys and, if the transaction is new, records it as RECEIVED.
     * Two concurrent claims for the same transaction must never both return NEW.
     */
    ClaimResult claim(TransactionRequest request, String fingerprint, Instant now);

    /**
     * Atomically moves a transaction to {@code to}.
     *
     * @throws com.fde.txnprocessor.domain.IllegalStatusTransitionException if the lifecycle forbids it
     * @throws java.util.NoSuchElementException if the transaction is unknown
     */
    TransactionRecord transition(String transactionId, TransactionStatus to, String reason, Instant now);

    Optional<TransactionRecord> find(String transactionId);

    /** All records ordered by sequence number, then transactionId. */
    List<TransactionRecord> findAll();

    default List<TransactionRecord> findByStatus(TransactionStatus status) {
        return findAll().stream().filter(r -> r.status() == status).toList();
    }
}

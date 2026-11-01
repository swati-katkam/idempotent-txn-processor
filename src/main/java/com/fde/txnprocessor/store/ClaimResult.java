package com.fde.txnprocessor.store;

import com.fde.txnprocessor.domain.TransactionRecord;

/**
 * Result of the atomic dedup gate.
 *
 * @param record the newly created record for NEW, otherwise the existing record that was matched
 */
public record ClaimResult(Outcome outcome, TransactionRecord record) {

    public enum Outcome {
        /** First time this transaction is seen; a RECEIVED record was created. */
        NEW,
        /** The exact requestId was seen before (client retry / redelivery). */
        DUPLICATE_REQUEST,
        /** A new requestId for a transactionId already known with an identical payload. */
        DUPLICATE_TRANSACTION,
        /** A known transactionId or requestId arrived with a different payload. Never processed. */
        CONFLICT
    }
}

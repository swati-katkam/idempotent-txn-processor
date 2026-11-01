package com.fde.txnprocessor.ordering;

import com.fde.txnprocessor.validation.TransactionValidator;

/**
 * How sequence numbers are interpreted.
 *
 * @param scope                          which requests share one sequence space
 * @param firstSequence                  the sequence number each partition starts at
 * @param rejectedRecordsReleaseSequence if true, a record that fails validation but carries a
 *                                       readable sequence number frees that slot, so one poison
 *                                       record cannot block its partition forever
 */
public record OrderingPolicy(Scope scope, long firstSequence, boolean rejectedRecordsReleaseSequence) {

    public enum Scope {
        /** One sequence across the whole stream, like a single queue partition or an outbox id. */
        GLOBAL,
        /** One sequence per account; accounts are processed independently. */
        ACCOUNT
    }

    public OrderingPolicy {
        if (firstSequence < 1) {
            throw new IllegalArgumentException("firstSequence must be >= 1");
        }
    }

    /** Partition key for an account, or null if the account id is unusable. */
    public String partitionFor(String accountId) {
        return switch (scope) {
            case GLOBAL -> "GLOBAL";
            case ACCOUNT -> TransactionValidator.isIdentifier(accountId) ? accountId.trim() : null;
        };
    }
}

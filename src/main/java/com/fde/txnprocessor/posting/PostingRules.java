package com.fde.txnprocessor.posting;

import com.fde.txnprocessor.domain.TransactionType;

import java.math.BigDecimal;
import java.util.function.Function;
import java.util.function.Predicate;

/** Pure business rules for posting. No state, no I/O, so they are trivially unit-testable. */
public final class PostingRules {

    private PostingRules() {
    }

    /**
     * Signed balance change for an entry.
     *
     * @param postedEntries lookup of already-posted entries by transactionId (for reversals)
     * @param isReversed    whether a posted transaction has already been reversed
     */
    public static BigDecimal balanceDelta(LedgerEntry entry, Function<String, LedgerEntry> postedEntries,
                                          Predicate<String> isReversed) {
        return switch (entry.type()) {
            case CREDIT -> entry.amount();
            case DEBIT -> entry.amount().negate();
            case REVERSAL -> reversalDelta(entry, postedEntries, isReversed);
        };
    }

    public static void requireNonNegative(BigDecimal resultingBalance) {
        if (resultingBalance.signum() < 0) {
            throw new BusinessRuleViolation("INSUFFICIENT_FUNDS");
        }
    }

    private static BigDecimal reversalDelta(LedgerEntry reversal, Function<String, LedgerEntry> postedEntries,
                                            Predicate<String> isReversed) {
        LedgerEntry original = postedEntries.apply(reversal.originalTransactionId());
        if (original == null) {
            throw new BusinessRuleViolation("ORIGINAL_NOT_FOUND");
        }
        if (original.type() == TransactionType.REVERSAL) {
            throw new BusinessRuleViolation("CANNOT_REVERSE_REVERSAL");
        }
        if (!original.accountId().equals(reversal.accountId()) || !original.currency().equals(reversal.currency())) {
            throw new BusinessRuleViolation("ORIGINAL_ACCOUNT_MISMATCH");
        }
        if (original.amount().compareTo(reversal.amount()) != 0) {
            throw new BusinessRuleViolation("REVERSAL_AMOUNT_MISMATCH");
        }
        if (isReversed.test(original.transactionId())) {
            throw new BusinessRuleViolation("ALREADY_REVERSED");
        }
        return balanceDelta(original, postedEntries, isReversed).negate();
    }
}

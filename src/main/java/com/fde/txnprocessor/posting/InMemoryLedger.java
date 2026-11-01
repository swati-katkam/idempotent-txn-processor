package com.fde.txnprocessor.posting;

import com.fde.txnprocessor.domain.TransactionType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * In-memory system of record. Each {@link #post} is one atomic unit of work: the new balance is
 * computed and every rule is checked before anything is mutated, then the journal entry, the
 * balance and the reversal marker are written together under one lock. A rule violation therefore
 * leaves no partial state. The {@code entriesByTransactionId} map plays the role of a unique
 * constraint and is what makes posting idempotent.
 */
public final class InMemoryLedger implements PostingGateway {

    public record AccountKey(String accountId, String currency) {
    }

    private final Map<String, LedgerEntry> entriesByTransactionId = new HashMap<>();
    private final List<LedgerEntry> journal = new ArrayList<>();
    private final Map<AccountKey, BigDecimal> balances = new LinkedHashMap<>();
    private final Set<String> reversedTransactionIds = new HashSet<>();

    @Override
    public synchronized PostingResult post(LedgerEntry entry) {
        LedgerEntry existing = entriesByTransactionId.get(entry.transactionId());
        if (existing != null) {
            if (!existing.samePostingAs(entry)) {
                throw new BusinessRuleViolation("IDEMPOTENCY_CONFLICT");
            }
            return PostingResult.ALREADY_APPLIED;
        }

        AccountKey key = new AccountKey(entry.accountId(), entry.currency());
        BigDecimal delta = PostingRules.balanceDelta(entry, entriesByTransactionId::get, reversedTransactionIds::contains);
        BigDecimal newBalance = balances.getOrDefault(key, BigDecimal.ZERO).add(delta);
        PostingRules.requireNonNegative(newBalance);

        entriesByTransactionId.put(entry.transactionId(), entry);
        journal.add(entry);
        balances.put(key, newBalance);
        if (entry.type() == TransactionType.REVERSAL) {
            reversedTransactionIds.add(entry.originalTransactionId());
        }
        return PostingResult.APPLIED;
    }

    public synchronized BigDecimal balance(String accountId, String currency) {
        return balances.getOrDefault(new AccountKey(accountId, currency), BigDecimal.ZERO);
    }

    public synchronized Map<AccountKey, BigDecimal> balances() {
        return new LinkedHashMap<>(balances);
    }

    /** Entries in the order they were posted. */
    public synchronized List<LedgerEntry> journal() {
        return List.copyOf(journal);
    }
}

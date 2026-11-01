package com.fde.txnprocessor;

import com.fde.txnprocessor.config.ProcessorConfig;
import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.ordering.OrderingPolicy;
import com.fde.txnprocessor.support.Harness;
import com.fde.txnprocessor.support.Raw;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderingTest {

    @Test
    void fullyReversedArrivalIsAppliedInSequenceOrder() {
        Harness h = new Harness().run(
                Raw.txn("TXN-3", "REQ-3", 3).credit("3"),
                Raw.txn("TXN-2", "REQ-2", 2).credit("2"),
                Raw.txn("TXN-1", "REQ-1", 1).credit("1"));

        assertEquals(List.of("TXN-1", "TXN-2", "TXN-3"), h.postedTransactionIds());
    }

    @Test
    void debitArrivingBeforeItsFundingCreditDoesNotFailForInsufficientFunds() {
        Harness h = new Harness().run(
                Raw.txn("TXN-2", "REQ-2", 2).debit("80"),
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"));

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-2"));
        assertEquals(new BigDecimal("20"), h.balance("ACC-1"));
    }

    @Test
    void gapKeepsLaterSequencesPendingAndIsReported() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("10"),
                Raw.txn("TXN-3", "REQ-3", 3).credit("30"),
                Raw.txn("TXN-4", "REQ-4", 4).credit("40"));

        assertEquals(TransactionStatus.PENDING, h.status("TXN-3"));
        assertEquals(TransactionStatus.PENDING, h.status("TXN-4"));
        assertEquals("AWAITING_EARLIER_SEQUENCE", h.record("TXN-3").reason());
        assertEquals(Map.of("GLOBAL", List.of(2L)), h.module.report().missingSequences());
    }

    @Test
    void lateArrivalOfTheMissingSequenceReleasesEverythingBehindIt() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("10"),
                Raw.txn("TXN-3", "REQ-3", 3).credit("30"),
                Raw.txn("TXN-4", "REQ-4", 4).credit("40"));

        h.run(Raw.txn("TXN-2", "REQ-2", 2).credit("20"));

        assertEquals(List.of("TXN-1", "TXN-2", "TXN-3", "TXN-4"), h.postedTransactionIds());
        assertTrue(h.module.report().missingSequences().isEmpty());
    }

    @Test
    void newTransactionReusingAConsumedSequenceIsRejected() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("10"),
                Raw.txn("TXN-9", "REQ-9", 1).credit("99"));

        assertEquals(TransactionStatus.REJECTED, h.status("TXN-9"));
        assertEquals("SEQUENCE_CONFLICT", h.record("TXN-9").reason());
        assertEquals(List.of("TXN-1"), h.postedTransactionIds());
    }

    @Test
    void twoTransactionsClaimingTheSameFutureSequenceOnlyFirstIsKept() {
        Harness h = new Harness().run(
                Raw.txn("TXN-2", "REQ-2", 2).credit("20"),
                Raw.txn("TXN-X", "REQ-X", 2).credit("99"),
                Raw.txn("TXN-1", "REQ-1", 1).credit("10"));

        assertEquals(TransactionStatus.REJECTED, h.status("TXN-X"));
        assertEquals(List.of("TXN-1", "TXN-2"), h.postedTransactionIds());
    }

    @Test
    void invalidRecordDoesNotBlockTheSequenceBehindIt() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("10"),
                Raw.txn("TXN-3", "REQ-3", 3).credit("30"),
                Raw.txn("TXN-2", "REQ-2", 2).amount("not-a-number"));

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-3"));
    }

    @Test
    void invalidRecordBlocksWhenSlotReleaseIsDisabled() {
        ProcessorConfig config = ProcessorConfig.defaults()
                .withOrderingPolicy(new OrderingPolicy(OrderingPolicy.Scope.GLOBAL, 1, false));
        Harness h = new Harness(config).run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("10"),
                Raw.txn("TXN-2", "REQ-2", 2).amount("-5"),
                Raw.txn("TXN-3", "REQ-3", 3).credit("30"));

        assertEquals(TransactionStatus.PENDING, h.status("TXN-3"));
    }

    @Test
    void perAccountScopeOrdersAccountsIndependently() {
        ProcessorConfig config = ProcessorConfig.defaults()
                .withOrderingPolicy(new OrderingPolicy(OrderingPolicy.Scope.ACCOUNT, 1, true));
        Harness h = new Harness(config).run(
                Raw.txn("A-2", "REQ-A2", 2).account("ACC-A").debit("5"),
                Raw.txn("B-1", "REQ-B1", 1).account("ACC-B").credit("7"),
                Raw.txn("A-1", "REQ-A1", 1).account("ACC-A").credit("10"));

        // B-1 was not held back by account A's gap
        assertEquals(List.of("B-1", "A-1", "A-2"), h.postedTransactionIds());
        assertEquals(new BigDecimal("5"), h.balance("ACC-A"));
    }
}

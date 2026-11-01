package com.fde.txnprocessor;

import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.support.Harness;
import com.fde.txnprocessor.support.Raw;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BusinessRulesTest {

    @Test
    void reversalUndoesTheOriginalPosting() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-2", "REQ-2", 2).debit("30"),
                Raw.txn("TXN-3", "REQ-3", 3).reversal("30", "TXN-2"));

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-3"));
        assertEquals(new BigDecimal("100"), h.balance("ACC-1"));
    }

    @Test
    void transactionCannotBeReversedTwice() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-2", "REQ-2", 2).reversal("100", "TXN-1"),
                Raw.txn("TXN-3", "REQ-3", 3).reversal("100", "TXN-1"));

        assertEquals(TransactionStatus.FAILED, h.status("TXN-3"));
        assertEquals("ALREADY_REVERSED", h.record("TXN-3").reason());
        assertEquals(new BigDecimal("0"), h.balance("ACC-1"));
    }

    @Test
    void reversalOfUnknownOrFailedTransactionFails() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).debit("10"),
                Raw.txn("TXN-2", "REQ-2", 2).reversal("10", "TXN-1"));

        assertEquals("INSUFFICIENT_FUNDS", h.record("TXN-1").reason());
        assertEquals("ORIGINAL_NOT_FOUND", h.record("TXN-2").reason());
    }

    @Test
    void reversalMustMatchOriginalAmountAndAccount() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-2", "REQ-2", 2).reversal("50", "TXN-1"),
                Raw.txn("TXN-3", "REQ-3", 3).account("ACC-OTHER").reversal("100", "TXN-1"));

        assertEquals("REVERSAL_AMOUNT_MISMATCH", h.record("TXN-2").reason());
        assertEquals("ORIGINAL_ACCOUNT_MISMATCH", h.record("TXN-3").reason());
    }

    @Test
    void failedDebitLeavesNoPartialState() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("50"),
                Raw.txn("TXN-2", "REQ-2", 2).debit("50.01"));

        assertEquals(new BigDecimal("50"), h.balance("ACC-1"));
        assertEquals(1, h.postedTransactionIds().size());
    }
}

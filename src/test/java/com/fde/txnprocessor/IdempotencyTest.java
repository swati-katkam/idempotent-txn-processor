package com.fde.txnprocessor;

import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.observability.ProcessingMetrics.Counter;
import com.fde.txnprocessor.processing.RequestAudit;
import com.fde.txnprocessor.support.Harness;
import com.fde.txnprocessor.support.Raw;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotencyTest {

    @Test
    void sameRequestDeliveredThreeTimesPostsOnce() {
        Raw request = Raw.txn("TXN-1", "REQ-1", 1).credit("100");
        Harness h = new Harness().run(request, request, request);

        assertEquals(List.of("TXN-1"), h.postedTransactionIds());
        assertEquals(new BigDecimal("100"), h.balance("ACC-1"));
        assertEquals(2, h.count(Counter.DUPLICATE));
        assertEquals("matchedOn=REQUEST_ID originalStatus=PROCESSED", h.module.requestAudit().get(1).detail());
    }

    @Test
    void newRequestIdForSameTransactionIsDetectedAsDuplicate() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-1", "REQ-1-RETRY", 1).credit("100.00"));

        assertEquals(List.of("TXN-1"), h.postedTransactionIds());
        RequestAudit duplicate = h.module.requestAudit().get(1);
        assertEquals(RequestAudit.Decision.DUPLICATE, duplicate.decision());
        assertTrue(duplicate.detail().contains("TRANSACTION_ID"));
    }

    @Test
    void sameTransactionIdWithDifferentPayloadIsRejectedAndOriginalIsUntouched() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-1", "REQ-1-X", 1).credit("999"));

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-1"));
        assertEquals(new BigDecimal("100"), h.balance("ACC-1"));
        assertEquals(List.of("IDEMPOTENCY_KEY_CONFLICT"), h.module.rejections().findAll().get(0).reasons());
    }

    @Test
    void reusedRequestIdForADifferentTransactionIsRejected() {
        Harness h = new Harness().run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-2", "REQ-1", 2).credit("50"));

        assertTrue(h.module.store().find("TXN-2").isEmpty());
        assertEquals(List.of("TXN-1"), h.postedTransactionIds());
        assertEquals(List.of("IDEMPOTENCY_KEY_CONFLICT"), h.module.rejections().findAll().get(0).reasons());
    }

    @Test
    void duplicateArrivingWhileOriginalIsStillPendingIsNotProcessedTwice() {
        Harness h = new Harness().run(
                Raw.txn("TXN-2", "REQ-2", 2).debit("40"),
                Raw.txn("TXN-2", "REQ-2-DUP", 2).debit("40"),
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"));

        assertEquals("matchedOn=TRANSACTION_ID originalStatus=PENDING", h.module.requestAudit().get(1).detail());
        assertEquals(List.of("TXN-1", "TXN-2"), h.postedTransactionIds());
        assertEquals(new BigDecimal("60"), h.balance("ACC-1"));
    }

    @Test
    void replayingAWholeBatchChangesNothing() {
        Raw[] batch = {
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-2", "REQ-2", 2).debit("30"),
                Raw.txn("TXN-3", "REQ-3", 3).credit("5")
        };
        Harness h = new Harness().run(batch);
        BigDecimal balanceAfterFirstRun = h.balance("ACC-1");

        h.run(batch);

        assertEquals(balanceAfterFirstRun, h.balance("ACC-1"));
        assertEquals(3, h.postedTransactionIds().size());
        assertEquals(3, h.count(Counter.DUPLICATE));
    }
}

package com.fde.txnprocessor;

import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.observability.ProcessingMetrics.Counter;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.support.Harness;
import com.fde.txnprocessor.support.Raw;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The L2 suggested test data (section 9.8), end to end through the background worker. */
class SuggestedDatasetTest {

    private Harness runSuggestedDataset() {
        return Harness.withFaults("TXN-2005:1:BEFORE_COMMIT").run(
                Raw.txn("TXN-2002", "REQ-2", 2).account("ACC-201").debit("40"),
                Raw.txn("TXN-2001", "REQ-1", 1).account("ACC-201").credit("100"),
                Raw.txn("TXN-2002", "REQ-2-DUP", 2).account("ACC-201").debit("40"),
                Raw.txn("TXN-2003", "REQ-3", 3).account("ACC-201").credit("25"),
                Raw.txn("TXN-2004", "REQ-4", 4).account("ACC-202").debit("-10"),
                Raw.txn("TXN-2005", "REQ-5", 5).account("ACC-203").credit("500"));
    }

    @Test
    void producesTheExpectedOutcomeForEveryRow() {
        Harness h = runSuggestedDataset();

        // row 1: arrived before its predecessor, parked, then processed once sequence 1 landed
        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-2002"));
        assertEquals("RECEIVED>PENDING>PROCESSING>PROCESSED", h.record("TXN-2002").lifecycle());
        // row 2
        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-2001"));
        // row 3: duplicate, no second posting
        assertEquals(1, h.count(Counter.DUPLICATE));
        // row 4
        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-2003"));
        // row 5: failed validation, never became a transaction
        assertTrue(h.module.store().find("TXN-2004").isEmpty());
        assertEquals(List.of("amount:MUST_BE_POSITIVE"), h.module.rejections().findAll().get(0).reasons());
        // row 6: transient failure, retried once, then processed
        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-2005"));
        assertEquals(2, h.record("TXN-2005").attempts());
        assertEquals("RECEIVED>PROCESSING>RETRY_PENDING>PROCESSING>PROCESSED", h.record("TXN-2005").lifecycle());
    }

    @Test
    void appliesEachSideEffectExactlyOnceInSequenceOrder() {
        Harness h = runSuggestedDataset();

        assertEquals(List.of("TXN-2001", "TXN-2002", "TXN-2003", "TXN-2005"), h.postedTransactionIds());
        assertEquals(new BigDecimal("85"), h.balance("ACC-201"));
        assertEquals(new BigDecimal("500"), h.balance("ACC-203"));
        assertEquals(BigDecimal.ZERO, h.balance("ACC-202"));
    }

    @Test
    void summaryCountsEveryRequestByFinalStatus() {
        ProcessingReport report = runSuggestedDataset().module.report();

        assertEquals(6, report.requestsReceived());
        assertEquals(4L, report.requestsByFinalStatus().get("PROCESSED"));
        assertEquals(1L, report.requestsByFinalStatus().get("DUPLICATE"));
        assertEquals(1L, report.requestsByFinalStatus().get("REJECTED"));
        assertEquals(0L, report.requestsByFinalStatus().get("FAILED"));
        assertEquals(0L, report.requestsByFinalStatus().get("PENDING"));
        assertEquals(1L, report.counters().get("RETRY_ATTEMPTED"));
        assertTrue(report.missingSequences().isEmpty());
    }

    @Test
    void reportNeverContainsUnmaskedAccountIds() {
        ProcessingReport report = runSuggestedDataset().module.report();

        String text = report.toText() + report.toJson();
        assertTrue(!text.contains("ACC-201") && !text.contains("ACC-203"), "account ids must be masked");
        assertTrue(text.contains("****201"));
    }
}

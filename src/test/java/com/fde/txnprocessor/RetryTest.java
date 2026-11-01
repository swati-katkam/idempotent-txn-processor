package com.fde.txnprocessor;

import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.observability.ProcessingMetrics.Counter;
import com.fde.txnprocessor.retry.RetryPolicy;
import com.fde.txnprocessor.support.Harness;
import com.fde.txnprocessor.support.Raw;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryTest {

    @Test
    void transientFailureIsRetriedAndSucceeds() {
        Harness h = Harness.withFaults("TXN-1:2").run(Raw.txn("TXN-1", "REQ-1", 1).credit("100"));

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-1"));
        assertEquals(3, h.record("TXN-1").attempts());
        assertEquals(2, h.count(Counter.RETRY_ATTEMPTED));
        assertEquals(List.of("TXN-1"), h.postedTransactionIds());
    }

    @Test
    void retriesAreExhaustedThenFailedWithNoSideEffect() {
        Harness h = Harness.withFaults("TXN-1:10").run(Raw.txn("TXN-1", "REQ-1", 1).credit("100"));

        assertEquals(TransactionStatus.FAILED, h.status("TXN-1"));
        assertEquals("RETRIES_EXHAUSTED:TRANSIENT_POSTING_FAILURE", h.record("TXN-1").reason());
        assertEquals(3, h.record("TXN-1").attempts());
        assertTrue(h.postedTransactionIds().isEmpty());
        assertEquals(BigDecimal.ZERO, h.balance("ACC-1"));
    }

    @Test
    void lostAcknowledgementAfterCommitDoesNotDoublePost() {
        Harness h = Harness.withFaults("TXN-1:1:AFTER_COMMIT").run(Raw.txn("TXN-1", "REQ-1", 1).credit("100"));

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-1"));
        assertEquals("ALREADY_APPLIED", h.record("TXN-1").reason());
        assertEquals(List.of("TXN-1"), h.postedTransactionIds());
        assertEquals(new BigDecimal("100"), h.balance("ACC-1"));
        assertEquals(1, h.count(Counter.IDEMPOTENT_REPLAY));
    }

    @Test
    void laterSequencesWaitWhileEarlierOneIsRetrying() {
        Harness h = Harness.withFaults("TXN-1:1");
        h.ingest(Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-2", "REQ-2", 2).debit("60"));

        assertEquals(TransactionStatus.RETRY_PENDING, h.status("TXN-1"));
        assertEquals(TransactionStatus.PENDING, h.status("TXN-2"));

        h.clock.advance(Duration.ofSeconds(1));
        h.module.processor().runDueRetries();

        assertEquals(List.of("TXN-1", "TXN-2"), h.postedTransactionIds());
        assertEquals(new BigDecimal("40"), h.balance("ACC-1"));
    }

    @Test
    void exhaustedFailureReleasesTheNextSequence() {
        Harness h = Harness.withFaults("TXN-1:10").run(
                Raw.txn("TXN-1", "REQ-1", 1).credit("100"),
                Raw.txn("TXN-2", "REQ-2", 2).account("ACC-2").credit("5"));

        assertEquals(TransactionStatus.FAILED, h.status("TXN-1"));
        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-2"));
    }

    @Test
    void businessRuleFailureIsNotRetried() {
        Harness h = new Harness().run(Raw.txn("TXN-1", "REQ-1", 1).debit("10"));

        assertEquals(TransactionStatus.FAILED, h.status("TXN-1"));
        assertEquals("INSUFFICIENT_FUNDS", h.record("TXN-1").reason());
        assertEquals(1, h.record("TXN-1").attempts());
        assertEquals(0, h.count(Counter.RETRY_SCHEDULED));
    }

    @Test
    void operatorCanReplayAFailedTransactionOnceTheCauseIsFixed() {
        Harness h = Harness.withFaults("TXN-1:10").run(Raw.txn("TXN-1", "REQ-1", 1).credit("100"));
        assertEquals(TransactionStatus.FAILED, h.status("TXN-1"));

        h.module.postingGateway().clear("TXN-1");
        h.module.processor().replayFailed("TXN-1");
        h.module.processor().runDueRetries();

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-1"));
        assertTrue(h.record("TXN-1").lifecycle().endsWith("FAILED>RETRY_PENDING>PROCESSING>PROCESSED"));
        assertEquals(new BigDecimal("100"), h.balance("ACC-1"));
    }

    @Test
    void backoffGrowsExponentiallyAndIsCapped() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofMillis(100), 2.0, Duration.ofMillis(300));

        assertEquals(Duration.ofMillis(100), policy.backoffAfter(1));
        assertEquals(Duration.ofMillis(200), policy.backoffAfter(2));
        assertEquals(Duration.ofMillis(300), policy.backoffAfter(3));
        assertTrue(policy.canRetry(4));
        assertTrue(!policy.canRetry(5));
    }
}

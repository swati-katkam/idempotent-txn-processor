package com.fde.txnprocessor;

import com.fde.txnprocessor.domain.IllegalStatusTransitionException;
import com.fde.txnprocessor.domain.TransactionRecord;
import com.fde.txnprocessor.domain.TransactionRequest;
import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.domain.TransactionType;
import com.fde.txnprocessor.store.ClaimResult;
import com.fde.txnprocessor.store.InMemoryTransactionStateStore;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StateAndConcurrencyTest {

    private static final Instant NOW = Instant.parse("2026-11-01T09:00:00Z");

    private static TransactionRequest request(String requestId) {
        return new TransactionRequest("TXN-1", requestId, "ACC-1", TransactionType.CREDIT, new BigDecimal("10"),
                "USD", 1, null, null, "test");
    }

    @Test
    void illegalTransitionsAreRefused() {
        TransactionRecord received = TransactionRecord.received(request("REQ-1"), "fp", NOW);
        TransactionRecord processed = received
                .transitionTo(TransactionStatus.PROCESSING, null, NOW)
                .transitionTo(TransactionStatus.PROCESSED, null, NOW);

        assertThrows(IllegalStatusTransitionException.class,
                () -> received.transitionTo(TransactionStatus.PROCESSED, null, NOW));
        assertThrows(IllegalStatusTransitionException.class,
                () -> processed.transitionTo(TransactionStatus.PROCESSING, null, NOW));
        assertThrows(IllegalStatusTransitionException.class,
                () -> processed.transitionTo(TransactionStatus.RETRY_PENDING, null, NOW));
    }

    @Test
    void historyRecordsEveryTransition() {
        TransactionRecord record = TransactionRecord.received(request("REQ-1"), "fp", NOW)
                .transitionTo(TransactionStatus.PROCESSING, null, NOW)
                .transitionTo(TransactionStatus.RETRY_PENDING, "TRANSIENT", NOW)
                .transitionTo(TransactionStatus.PROCESSING, null, NOW);

        assertEquals(4, record.history().size());
        assertEquals(2, record.attempts());
        assertEquals("TRANSIENT", record.history().get(2).reason());
    }

    @Test
    void concurrentClaimsForTheSameTransactionProduceExactlyOneWinner() throws Exception {
        InMemoryTransactionStateStore store = new InMemoryTransactionStateStore();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ClaimResult.Outcome>> results = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                TransactionRequest req = request("REQ-" + i);
                Callable<ClaimResult.Outcome> claim = () -> {
                    start.await();
                    return store.claim(req, req.businessFingerprint(), NOW).outcome();
                };
                results.add(pool.submit(claim));
            }
            start.countDown();
            long winners = 0;
            for (Future<ClaimResult.Outcome> result : results) {
                if (result.get() == ClaimResult.Outcome.NEW) {
                    winners++;
                }
            }
            assertEquals(1, winners);
        } finally {
            pool.shutdownNow();
        }
    }
}

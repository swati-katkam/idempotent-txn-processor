package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.domain.RawTransactionRequest;
import com.fde.txnprocessor.ingest.TransactionSource;
import com.fde.txnprocessor.observability.EventLog;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The background job: poll the source, hand each record to the processor, run due retries, and
 * idle until the next record or retry. Stops when the source is exhausted and no retries remain
 * (batch mode), or when {@link #stop()} is called (graceful shutdown).
 */
public final class TransactionWorker implements Runnable {

    private static final EventLog LOG = EventLog.forClass(TransactionWorker.class);

    private final TransactionSource source;
    private final TransactionProcessor processor;
    private final int batchSize;
    private final Duration idlePollInterval;
    private final Clock clock;
    private final Sleeper sleeper;
    private volatile boolean running = true;

    public TransactionWorker(TransactionSource source, TransactionProcessor processor, int batchSize,
                             Duration idlePollInterval, Clock clock, Sleeper sleeper) {
        this.source = source;
        this.processor = processor;
        this.batchSize = batchSize;
        this.idlePollInterval = idlePollInterval;
        this.clock = clock;
        this.sleeper = sleeper;
    }

    @Override
    public void run() {
        LOG.info("WORKER_STARTED", "batchSize", batchSize, "idlePollMs", idlePollInterval.toMillis());
        String stopReason = "STOP_REQUESTED";
        try {
            while (running) {
                List<RawTransactionRequest> batch = source.poll(batchSize);
                for (RawTransactionRequest raw : batch) {
                    ingestIsolated(raw);
                }
                processor.runDueRetries();
                if (batch.isEmpty()) {
                    if (source.isExhausted() && !processor.hasScheduledRetries()) {
                        stopReason = "SOURCE_DRAINED";
                        break;
                    }
                    sleeper.sleep(idleWait());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopReason = "INTERRUPTED";
        } finally {
            LOG.info("WORKER_STOPPED", "reason", stopReason);
        }
    }

    public void stop() {
        running = false;
    }

    /** One bad record must never stop the batch. */
    private void ingestIsolated(RawTransactionRequest raw) {
        try {
            processor.ingest(raw);
        } catch (RuntimeException e) {
            processor.quarantine(raw, e);
        }
    }

    private Duration idleWait() {
        Optional<Instant> nextRetry = processor.nextRetryDueAt();
        if (nextRetry.isEmpty()) {
            return idlePollInterval;
        }
        Duration untilRetry = Duration.between(clock.instant(), nextRetry.get());
        if (untilRetry.isNegative()) {
            return Duration.ZERO;
        }
        return untilRetry.compareTo(idlePollInterval) < 0 ? untilRetry : idlePollInterval;
    }
}

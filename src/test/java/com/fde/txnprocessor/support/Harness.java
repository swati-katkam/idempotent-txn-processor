package com.fde.txnprocessor.support;

import com.fde.txnprocessor.TransactionProcessingModule;
import com.fde.txnprocessor.config.ProcessorConfig;
import com.fde.txnprocessor.domain.TransactionRecord;
import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.ingest.InMemoryTransactionSource;
import com.fde.txnprocessor.observability.ProcessingMetrics.Counter;
import com.fde.txnprocessor.posting.LedgerEntry;
import com.fde.txnprocessor.simulation.FaultSpec;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/** Builds a fully wired module on a fake clock and runs batches through the real worker loop. */
public final class Harness {

    public final MutableClock clock = new MutableClock(Instant.parse("2026-11-01T09:00:00Z"));
    public final TransactionProcessingModule module;

    public Harness() {
        this(ProcessorConfig.defaults());
    }

    public Harness(ProcessorConfig config) {
        this.module = TransactionProcessingModule.create(config, clock);
    }

    public static Harness withFaults(String... faults) {
        return new Harness(ProcessorConfig.defaults().withFaults(Arrays.stream(faults).map(FaultSpec::parse).toList()));
    }

    /** Runs the background worker until the batch is drained and all retries are resolved. */
    public Harness run(Raw... requests) {
        InMemoryTransactionSource source = InMemoryTransactionSource.completedWith(
                Arrays.stream(requests).map(Raw::build).toList());
        module.newWorker(source, clock::advance).run();
        return this;
    }

    /** Ingests directly, without the worker loop, so tests can inspect intermediate state. */
    public Harness ingest(Raw... requests) {
        for (Raw request : requests) {
            module.processor().ingest(request.build());
        }
        return this;
    }

    public TransactionStatus status(String transactionId) {
        return record(transactionId).status();
    }

    public TransactionRecord record(String transactionId) {
        return module.store().find(transactionId).orElseThrow();
    }

    public BigDecimal balance(String accountId) {
        return module.ledger().balance(accountId, "USD");
    }

    public List<String> postedTransactionIds() {
        return module.ledger().journal().stream().map(LedgerEntry::transactionId).toList();
    }

    public long count(Counter counter) {
        return module.metrics().get(counter);
    }
}

package com.fde.txnprocessor;

import com.fde.txnprocessor.config.ProcessorConfig;
import com.fde.txnprocessor.domain.TransactionRecord;
import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.ingest.TransactionSource;
import com.fde.txnprocessor.observability.Masking;
import com.fde.txnprocessor.observability.ProcessingMetrics;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.ordering.SequenceOrderingBuffer;
import com.fde.txnprocessor.posting.InMemoryLedger;
import com.fde.txnprocessor.processing.RequestAudit;
import com.fde.txnprocessor.processing.Sleeper;
import com.fde.txnprocessor.processing.TransactionProcessor;
import com.fde.txnprocessor.processing.TransactionWorker;
import com.fde.txnprocessor.retry.RetryScheduler;
import com.fde.txnprocessor.simulation.FaultInjectingPostingGateway;
import com.fde.txnprocessor.store.InMemoryRejectionStore;
import com.fde.txnprocessor.store.InMemoryTransactionStateStore;
import com.fde.txnprocessor.validation.TransactionValidator;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Composition root: wires the in-memory adapters to the core. Swapping to production adapters
 * (JDBC store, Kafka source, HTTP posting client) only touches this class.
 */
public final class TransactionProcessingModule {

    private final ProcessorConfig config;
    private final Clock clock;
    private final InMemoryLedger ledger = new InMemoryLedger();
    private final FaultInjectingPostingGateway postingGateway = new FaultInjectingPostingGateway(ledger);
    private final InMemoryTransactionStateStore store = new InMemoryTransactionStateStore();
    private final InMemoryRejectionStore rejections = new InMemoryRejectionStore();
    private final ProcessingMetrics metrics = new ProcessingMetrics();
    private final SequenceOrderingBuffer ordering;
    private final TransactionProcessor processor;

    private TransactionProcessingModule(ProcessorConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
        this.ordering = new SequenceOrderingBuffer(config.orderingPolicy());
        config.faults().forEach(postingGateway::inject);
        this.processor = new TransactionProcessor(new TransactionValidator(config.maxAmount()), store, rejections,
                ordering, postingGateway, config.retryPolicy(), new RetryScheduler(), metrics, clock);
    }

    public static TransactionProcessingModule create(ProcessorConfig config, Clock clock) {
        return new TransactionProcessingModule(config, clock);
    }

    public TransactionWorker newWorker(TransactionSource source, Sleeper sleeper) {
        return new TransactionWorker(source, processor, config.batchSize(), config.idlePollInterval(), clock, sleeper);
    }

    public ProcessingReport report() {
        List<TransactionRecord> records = store.findAll();

        Map<String, Long> requestsByFinalStatus = zeroedStatusCounts();
        List<ProcessingReport.RequestLine> requestLines = processor.requestAudit().stream().map(audit -> {
            String finalStatus = switch (audit.decision()) {
                case ACCEPTED -> store.find(audit.transactionId()).map(r -> r.status().name()).orElse("UNKNOWN");
                case DUPLICATE -> TransactionStatus.DUPLICATE.name();
                case REJECTED -> TransactionStatus.REJECTED.name();
            };
            requestsByFinalStatus.merge(finalStatus, 1L, Long::sum);
            return new ProcessingReport.RequestLine(audit.sourceRef(), audit.transactionId(), audit.requestId(),
                    audit.decision().name(), finalStatus, audit.detail());
        }).toList();

        Map<String, Long> transactionsByStatus = zeroedStatusCounts();
        transactionsByStatus.remove(TransactionStatus.DUPLICATE.name());
        records.forEach(r -> transactionsByStatus.merge(r.status().name(), 1L, Long::sum));

        List<ProcessingReport.TransactionLine> transactionLines = records.stream()
                .map(r -> new ProcessingReport.TransactionLine(r.transactionId(), Masking.account(r.request().accountId()),
                        r.request().type().name(), r.request().sequenceNumber(), r.status().name(), r.attempts(),
                        r.reason(), r.lifecycle()))
                .toList();

        Map<String, Long> counters = new LinkedHashMap<>();
        metrics.snapshot().forEach((counter, value) -> counters.put(counter.name(), value));

        Map<String, String> balances = new LinkedHashMap<>();
        ledger.balances().forEach((key, balance) ->
                balances.put(Masking.account(key.accountId()) + " " + key.currency(), balance.toPlainString()));

        Map<String, List<Long>> missing = new LinkedHashMap<>();
        ordering.missingSequences().forEach((partition, gaps) ->
                missing.put("GLOBAL".equals(partition) ? partition : Masking.account(partition), gaps));

        return new ProcessingReport(clock.instant(), requestLines.size(), requestsByFinalStatus, transactionsByStatus,
                counters, requestLines, transactionLines, rejections.findAll(), missing, balances);
    }

    public TransactionProcessor processor() {
        return processor;
    }

    public InMemoryTransactionStateStore store() {
        return store;
    }

    public InMemoryRejectionStore rejections() {
        return rejections;
    }

    public InMemoryLedger ledger() {
        return ledger;
    }

    public FaultInjectingPostingGateway postingGateway() {
        return postingGateway;
    }

    public ProcessingMetrics metrics() {
        return metrics;
    }

    public SequenceOrderingBuffer ordering() {
        return ordering;
    }

    public List<RequestAudit> requestAudit() {
        return processor.requestAudit();
    }

    private static Map<String, Long> zeroedStatusCounts() {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (TransactionStatus status : TransactionStatus.values()) {
            counts.put(status.name(), 0L);
        }
        return counts;
    }
}

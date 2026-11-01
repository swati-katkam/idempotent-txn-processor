package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.domain.RawTransactionRequest;
import com.fde.txnprocessor.domain.TransactionRecord;
import com.fde.txnprocessor.domain.TransactionRequest;
import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.observability.EventLog;
import com.fde.txnprocessor.observability.Masking;
import com.fde.txnprocessor.observability.ProcessingMetrics;
import com.fde.txnprocessor.observability.ProcessingMetrics.Counter;
import com.fde.txnprocessor.ordering.SequenceOrderingBuffer;
import com.fde.txnprocessor.posting.BusinessRuleViolation;
import com.fde.txnprocessor.posting.LedgerEntry;
import com.fde.txnprocessor.posting.PostingGateway;
import com.fde.txnprocessor.posting.PostingResult;
import com.fde.txnprocessor.posting.TransientPostingException;
import com.fde.txnprocessor.retry.RetryPolicy;
import com.fde.txnprocessor.retry.RetryScheduler;
import com.fde.txnprocessor.store.ClaimResult;
import com.fde.txnprocessor.store.RejectedRecord;
import com.fde.txnprocessor.store.RejectionStore;
import com.fde.txnprocessor.store.TransactionStateStore;
import com.fde.txnprocessor.validation.TransactionValidator;
import com.fde.txnprocessor.validation.ValidationError;
import com.fde.txnprocessor.validation.ValidationResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Business core of the service. It knows nothing about where requests come from; it is driven by
 * {@link TransactionWorker} (or any other caller) through {@link #ingest} and {@link #runDueRetries}.
 *
 * <p>Pipeline for each request:
 * <ol>
 *   <li>validate, and dead-letter it if malformed;</li>
 *   <li>claim the dedup keys atomically (requestId, then transactionId plus payload fingerprint);</li>
 *   <li>admit through the sequence buffer: process now, or park as PENDING;</li>
 *   <li>post through the idempotent gateway; on a transient error schedule a bounded retry;</li>
 *   <li>on a terminal outcome, free the sequence slot and process whatever it unblocks.</li>
 * </ol>
 *
 * <p>Methods are synchronized: one processor instance is a single logical consumer. Scaling out
 * means one processor per partition (see DESIGN.md), not concurrent access to one instance.
 */
public final class TransactionProcessor {

    private static final EventLog LOG = EventLog.forClass(TransactionProcessor.class);

    private final TransactionValidator validator;
    private final TransactionStateStore store;
    private final RejectionStore rejections;
    private final SequenceOrderingBuffer ordering;
    private final PostingGateway postingGateway;
    private final RetryPolicy retryPolicy;
    private final RetryScheduler retryScheduler;
    private final ProcessingMetrics metrics;
    private final Clock clock;
    private final List<RequestAudit> requestAudit = new ArrayList<>();

    public TransactionProcessor(TransactionValidator validator, TransactionStateStore store, RejectionStore rejections,
                                SequenceOrderingBuffer ordering, PostingGateway postingGateway, RetryPolicy retryPolicy,
                                RetryScheduler retryScheduler, ProcessingMetrics metrics, Clock clock) {
        this.validator = validator;
        this.store = store;
        this.rejections = rejections;
        this.ordering = ordering;
        this.postingGateway = postingGateway;
        this.retryPolicy = retryPolicy;
        this.retryScheduler = retryScheduler;
        this.metrics = metrics;
        this.clock = clock;
    }

    public synchronized void ingest(RawTransactionRequest raw) {
        metrics.increment(Counter.RECEIVED);
        ValidationResult validation = validator.validate(raw);
        if (validation instanceof ValidationResult.Invalid invalid) {
            reject(raw.sourceRef(), raw.transactionId(), raw.requestId(),
                    invalid.errors().stream().map(ValidationError::toString).toList());
            releaseSlotOfRejected(raw);
            return;
        }
        TransactionRequest request = ((ValidationResult.Valid) validation).request();
        ClaimResult claim = store.claim(request, request.businessFingerprint(), clock.instant());
        switch (claim.outcome()) {
            case NEW -> accept(request);
            case DUPLICATE_REQUEST, DUPLICATE_TRANSACTION -> duplicate(request, claim);
            case CONFLICT -> reject(request.sourceRef(), request.transactionId(), request.requestId(),
                    List.of("IDEMPOTENCY_KEY_CONFLICT"));
        }
    }

    /**
     * Isolates a request whose processing threw an unexpected error so the rest of the batch can
     * continue. Called by the worker; the request is dead-lettered with the error type only.
     */
    public synchronized void quarantine(RawTransactionRequest raw, RuntimeException error) {
        metrics.increment(Counter.INTERNAL_ERROR);
        LOG.error("TXN_INTERNAL_ERROR", "source", raw.sourceRef(), "txnId", raw.transactionId(),
                "error", error.getClass().getSimpleName());
        reject(raw.sourceRef(), raw.transactionId(), raw.requestId(),
                List.of("INTERNAL_ERROR:" + error.getClass().getSimpleName()));
    }

    /** Runs every retry whose backoff has elapsed. Returns how many were attempted. */
    public synchronized int runDueRetries() {
        List<String> due = retryScheduler.pollDue(clock.instant());
        for (String transactionId : due) {
            metrics.increment(Counter.RETRY_ATTEMPTED);
            try {
                processFrom(transactionId);
            } catch (RuntimeException e) {
                metrics.increment(Counter.INTERNAL_ERROR);
                LOG.error("TXN_RETRY_INTERNAL_ERROR", "txnId", transactionId, "error", e.getClass().getSimpleName());
            }
        }
        return due.size();
    }

    /**
     * Operator recovery: re-queue a FAILED transaction (e.g. after a downstream outage is fixed).
     * Safe to call repeatedly because posting is idempotent.
     */
    public synchronized void replayFailed(String transactionId) {
        store.transition(transactionId, TransactionStatus.RETRY_PENDING, "MANUAL_REPLAY", clock.instant());
        retryScheduler.schedule(transactionId, clock.instant());
        LOG.info("TXN_REPLAY_REQUESTED", "txnId", transactionId);
    }

    public synchronized boolean hasScheduledRetries() {
        return !retryScheduler.isEmpty();
    }

    public synchronized Optional<Instant> nextRetryDueAt() {
        return retryScheduler.nextDueAt();
    }

    public synchronized List<RequestAudit> requestAudit() {
        return List.copyOf(requestAudit);
    }

    // ---------------------------------------------------------------------------------------------

    private void accept(TransactionRequest request) {
        metrics.increment(Counter.ACCEPTED);
        requestAudit.add(new RequestAudit(request.sourceRef(), request.transactionId(), request.requestId(),
                RequestAudit.Decision.ACCEPTED, null));
        String partition = ordering.policy().partitionFor(request.accountId());
        String id = request.transactionId();

        switch (ordering.admit(partition, request.sequenceNumber(), id)) {
            case READY -> processFrom(id);
            case BUFFERED -> {
                long waitingFor = ordering.nextExpected(partition);
                metrics.increment(Counter.BUFFERED_OUT_OF_ORDER);
                // the gap that blocks it can move as other sequences arrive; the report shows the live gaps
                store.transition(id, TransactionStatus.PENDING, "AWAITING_EARLIER_SEQUENCE", clock.instant());
                LOG.info("TXN_PENDING", "txnId", id, "partition", maskPartition(partition),
                        "sequence", request.sequenceNumber(), "waitingFor", waitingFor);
            }
            case STALE, SLOT_TAKEN -> {
                metrics.increment(Counter.REJECTED);
                store.transition(id, TransactionStatus.REJECTED, "SEQUENCE_CONFLICT", clock.instant());
                LOG.warn("TXN_REJECTED", "txnId", id, "requestId", request.requestId(),
                        "sequence", request.sequenceNumber(), "reasons", "SEQUENCE_CONFLICT");
            }
        }
    }

    /** Processes a transaction and then every transaction its completion unblocks. */
    private void processFrom(String transactionId) {
        Optional<String> next = Optional.of(transactionId);
        while (next.isPresent()) {
            next = attempt(next.get());
        }
    }

    /** One processing attempt. Returns the transaction released by this one finishing, if any. */
    private Optional<String> attempt(String transactionId) {
        TransactionRecord record = store.transition(transactionId, TransactionStatus.PROCESSING, null, clock.instant());
        TransactionRequest request = record.request();
        LOG.info("TXN_PROCESSING", "txnId", transactionId, "attempt", record.attempts(),
                "account", Masking.account(request.accountId()), "type", request.type(),
                "sequence", request.sequenceNumber());

        PostingResult result;
        try {
            result = postingGateway.post(LedgerEntry.from(request));
        } catch (BusinessRuleViolation e) {
            return fail(record, e.code());
        } catch (RuntimeException e) {
            // TransientPostingException or an unclassified infrastructure error. Retrying is safe
            // because the gateway is idempotent on transactionId.
            return retryOrFail(record, e);
        }

        String reason = result == PostingResult.ALREADY_APPLIED ? "ALREADY_APPLIED" : null;
        if (result == PostingResult.ALREADY_APPLIED) {
            metrics.increment(Counter.IDEMPOTENT_REPLAY);
        }
        store.transition(transactionId, TransactionStatus.PROCESSED, reason, clock.instant());
        metrics.increment(Counter.PROCESSED);
        LOG.info("TXN_PROCESSED", "txnId", transactionId, "attempt", record.attempts(), "posting", result);
        return completeSlot(request);
    }

    private Optional<String> retryOrFail(TransactionRecord record, RuntimeException cause) {
        String error = cause instanceof TransientPostingException
                ? "TRANSIENT_POSTING_FAILURE"
                : "UNEXPECTED_" + cause.getClass().getSimpleName();
        if (!retryPolicy.canRetry(record.attempts())) {
            return fail(record, "RETRIES_EXHAUSTED:" + error);
        }
        Duration backoff = retryPolicy.backoffAfter(record.attempts());
        store.transition(record.transactionId(), TransactionStatus.RETRY_PENDING, error, clock.instant());
        retryScheduler.schedule(record.transactionId(), clock.instant().plus(backoff));
        metrics.increment(Counter.RETRY_SCHEDULED);
        LOG.warn("TXN_RETRY_SCHEDULED", "txnId", record.transactionId(), "attempt", record.attempts(),
                "maxAttempts", retryPolicy.maxAttempts(), "backoffMs", backoff.toMillis(), "error", error);
        // the sequence slot stays in flight, so later sequences in this partition keep waiting
        return Optional.empty();
    }

    private Optional<String> fail(TransactionRecord record, String reason) {
        store.transition(record.transactionId(), TransactionStatus.FAILED, reason, clock.instant());
        metrics.increment(Counter.FAILED);
        LOG.error("TXN_FAILED", "txnId", record.transactionId(), "attempt", record.attempts(), "reason", reason);
        return completeSlot(record.request());
    }

    private Optional<String> completeSlot(TransactionRequest request) {
        String partition = ordering.policy().partitionFor(request.accountId());
        Optional<String> released = ordering.complete(partition, request.sequenceNumber());
        released.ifPresent(next -> LOG.info("TXN_RELEASED", "txnId", next, "unblockedBy", request.transactionId()));
        return released;
    }

    private void duplicate(TransactionRequest request, ClaimResult claim) {
        metrics.increment(Counter.DUPLICATE);
        String matchedOn = claim.outcome() == ClaimResult.Outcome.DUPLICATE_REQUEST ? "REQUEST_ID" : "TRANSACTION_ID";
        TransactionStatus originalStatus = claim.record().status();
        requestAudit.add(new RequestAudit(request.sourceRef(), request.transactionId(), request.requestId(),
                RequestAudit.Decision.DUPLICATE, "matchedOn=" + matchedOn + " originalStatus=" + originalStatus));
        LOG.info("TXN_DUPLICATE", "txnId", request.transactionId(), "requestId", request.requestId(),
                "matchedOn", matchedOn, "originalStatus", originalStatus);
    }

    private void reject(String sourceRef, String transactionId, String requestId, List<String> reasons) {
        metrics.increment(Counter.REJECTED);
        rejections.add(new RejectedRecord(sourceRef, transactionId, requestId, reasons, clock.instant()));
        requestAudit.add(new RequestAudit(sourceRef, transactionId, requestId, RequestAudit.Decision.REJECTED,
                String.join(",", reasons)));
        LOG.warn("TXN_REJECTED", "source", sourceRef, "txnId", transactionId, "requestId", requestId,
                "reasons", String.join(",", reasons));
    }

    private void releaseSlotOfRejected(RawTransactionRequest raw) {
        if (!ordering.policy().rejectedRecordsReleaseSequence()) {
            return;
        }
        OptionalLong sequence = TransactionValidator.parseSequence(raw.sequenceNumber());
        String partition = ordering.policy().partitionFor(raw.accountId());
        if (sequence.isEmpty() || partition == null) {
            return;
        }
        ordering.skip(partition, sequence.getAsLong()).ifPresent(next -> {
            LOG.info("TXN_RELEASED", "txnId", next, "unblockedBy", "rejected:" + raw.sourceRef());
            processFrom(next);
        });
    }

    private static String maskPartition(String partition) {
        return "GLOBAL".equals(partition) ? partition : Masking.account(partition);
    }
}

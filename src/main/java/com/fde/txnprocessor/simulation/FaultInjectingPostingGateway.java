package com.fde.txnprocessor.simulation;

import com.fde.txnprocessor.posting.LedgerEntry;
import com.fde.txnprocessor.posting.PostingGateway;
import com.fde.txnprocessor.posting.PostingResult;
import com.fde.txnprocessor.posting.TransientPostingException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Decorator that injects transient failures in front of a real gateway, for demos and tests. */
public final class FaultInjectingPostingGateway implements PostingGateway {

    private final PostingGateway delegate;
    private final Map<String, FaultSpec> pendingFaults = new ConcurrentHashMap<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger injectedFailures = new AtomicInteger();

    public FaultInjectingPostingGateway(PostingGateway delegate) {
        this.delegate = delegate;
    }

    public void inject(FaultSpec fault) {
        pendingFaults.put(fault.transactionId(), fault);
    }

    public void clear(String transactionId) {
        pendingFaults.remove(transactionId);
    }

    @Override
    public PostingResult post(LedgerEntry entry) {
        calls.incrementAndGet();
        FaultSpec fault = consume(entry.transactionId());
        if (fault == null) {
            return delegate.post(entry);
        }
        injectedFailures.incrementAndGet();
        if (fault.mode() == FaultSpec.Mode.AFTER_COMMIT) {
            delegate.post(entry);
            throw new TransientPostingException("Simulated timeout after downstream commit");
        }
        throw new TransientPostingException("Simulated transient failure before downstream commit");
    }

    /** Total posting calls, including retries and injected failures. */
    public int calls() {
        return calls.get();
    }

    public int injectedFailures() {
        return injectedFailures.get();
    }

    private FaultSpec consume(String transactionId) {
        FaultSpec[] hit = new FaultSpec[1];
        pendingFaults.computeIfPresent(transactionId, (id, fault) -> {
            hit[0] = fault;
            return fault.failures() > 1 ? fault.decrement() : null;
        });
        return hit[0];
    }
}

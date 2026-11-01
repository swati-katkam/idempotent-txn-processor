package com.fde.txnprocessor.ingest;

import com.fde.txnprocessor.domain.RawTransactionRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Mock queue. Producers {@link #publish} records; {@link #complete()} signals that nothing more
 * will arrive. This is the seam where a Kafka, SQS or JMS consumer adapter would plug in.
 */
public final class InMemoryTransactionSource implements TransactionSource {

    private final Queue<RawTransactionRequest> queue = new ConcurrentLinkedQueue<>();
    private volatile boolean completed;

    /** A source pre-loaded with {@code records} and already marked complete. */
    public static InMemoryTransactionSource completedWith(Collection<RawTransactionRequest> records) {
        InMemoryTransactionSource source = new InMemoryTransactionSource();
        records.forEach(source::publish);
        source.complete();
        return source;
    }

    public void publish(RawTransactionRequest record) {
        if (completed) {
            throw new IllegalStateException("Source is already complete");
        }
        queue.add(record);
    }

    public void complete() {
        completed = true;
    }

    @Override
    public List<RawTransactionRequest> poll(int maxRecords) {
        List<RawTransactionRequest> batch = new ArrayList<>();
        RawTransactionRequest next;
        while (batch.size() < maxRecords && (next = queue.poll()) != null) {
            batch.add(next);
        }
        return batch;
    }

    @Override
    public boolean isExhausted() {
        return completed && queue.isEmpty();
    }
}

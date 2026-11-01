package com.fde.txnprocessor.observability;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Event counters. They count occurrences, unlike the report's status counts which show where each
 * request ended up, so a transaction that was retried twice adds 2 to RETRY_ATTEMPTED. In
 * production these map one-to-one to Micrometer counters.
 */
public final class ProcessingMetrics {

    public enum Counter {
        RECEIVED,
        ACCEPTED,
        DUPLICATE,
        REJECTED,
        BUFFERED_OUT_OF_ORDER,
        PROCESSED,
        IDEMPOTENT_REPLAY,
        RETRY_SCHEDULED,
        RETRY_ATTEMPTED,
        FAILED,
        INTERNAL_ERROR
    }

    private final Map<Counter, AtomicLong> counters = new EnumMap<>(Counter.class);

    public ProcessingMetrics() {
        for (Counter counter : Counter.values()) {
            counters.put(counter, new AtomicLong());
        }
    }

    public void increment(Counter counter) {
        counters.get(counter).incrementAndGet();
    }

    public long get(Counter counter) {
        return counters.get(counter).get();
    }

    public Map<Counter, Long> snapshot() {
        Map<Counter, Long> snapshot = new EnumMap<>(Counter.class);
        counters.forEach((counter, value) -> snapshot.put(counter, value.get()));
        return snapshot;
    }
}

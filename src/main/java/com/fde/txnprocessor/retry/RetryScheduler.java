package com.fde.txnprocessor.retry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * Due-time ordered queue of transactions waiting to be retried. In production this would be a
 * {@code next_attempt_at} column polled by the worker, or a delayed or retry topic.
 */
public final class RetryScheduler {

    private record Scheduled(String transactionId, Instant dueAt, long order) {
    }

    private final PriorityQueue<Scheduled> queue = new PriorityQueue<>(
            Comparator.comparing(Scheduled::dueAt).thenComparingLong(Scheduled::order));
    private long counter;

    public synchronized void schedule(String transactionId, Instant dueAt) {
        queue.add(new Scheduled(transactionId, dueAt, counter++));
    }

    public synchronized List<String> pollDue(Instant now) {
        List<String> due = new ArrayList<>();
        while (!queue.isEmpty() && !queue.peek().dueAt().isAfter(now)) {
            due.add(queue.poll().transactionId());
        }
        return due;
    }

    public synchronized Optional<Instant> nextDueAt() {
        return Optional.ofNullable(queue.peek()).map(Scheduled::dueAt);
    }

    public synchronized boolean isEmpty() {
        return queue.isEmpty();
    }
}

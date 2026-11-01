package com.fde.txnprocessor.ordering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Sequence-based ordering (option A). Within a partition, a transaction is only released for
 * processing once every lower sequence number has reached a terminal state. Later sequences wait in
 * a buffer, and gaps are reported.
 *
 * <p>At most one transaction per partition is in flight at a time. That is deliberate: a DEBIT must
 * not be evaluated against a balance that an earlier, still-retrying CREDIT has not yet reached.
 */
public final class SequenceOrderingBuffer {

    public enum Admission {
        /** This is the next expected sequence; process it now. */
        READY,
        /** An earlier sequence is still missing or in flight; wait. */
        BUFFERED,
        /** The sequence is lower than anything still open in the partition. */
        STALE,
        /** Another transaction already holds this sequence number. */
        SLOT_TAKEN
    }

    private enum SlotState { WAITING, IN_FLIGHT, DONE }

    private static final class Slot {
        private final String transactionId;
        private SlotState state;

        private Slot(String transactionId, SlotState state) {
            this.transactionId = transactionId;
            this.state = state;
        }
    }

    private static final class Partition {
        private long nextExpected;
        private final TreeMap<Long, Slot> slots = new TreeMap<>();

        private Partition(long firstSequence) {
            this.nextExpected = firstSequence;
        }
    }

    private static final int MAX_REPORTED_GAPS = 100;

    private final OrderingPolicy policy;
    private final Map<String, Partition> partitions = new HashMap<>();

    public SequenceOrderingBuffer(OrderingPolicy policy) {
        this.policy = policy;
    }

    public OrderingPolicy policy() {
        return policy;
    }

    public synchronized Admission admit(String partitionKey, long sequence, String transactionId) {
        Partition partition = partition(partitionKey);
        if (sequence < partition.nextExpected) {
            return Admission.STALE;
        }
        if (partition.slots.containsKey(sequence)) {
            return Admission.SLOT_TAKEN;
        }
        boolean ready = sequence == partition.nextExpected;
        partition.slots.put(sequence, new Slot(transactionId, ready ? SlotState.IN_FLIGHT : SlotState.WAITING));
        return ready ? Admission.READY : Admission.BUFFERED;
    }

    /**
     * Marks a transaction's slot as resolved (processed or permanently failed).
     *
     * @return the transaction that has just become ready, if any
     */
    public synchronized Optional<String> complete(String partitionKey, long sequence) {
        Partition partition = partitions.get(partitionKey);
        if (partition == null || sequence < partition.nextExpected) {
            return Optional.empty();
        }
        Slot slot = partition.slots.get(sequence);
        if (slot == null) {
            return Optional.empty();
        }
        slot.state = SlotState.DONE;
        return advance(partition);
    }

    /**
     * Resolves a slot that no valid transaction will ever fill (its record was rejected).
     *
     * @return the transaction that has just become ready, if any
     */
    public synchronized Optional<String> skip(String partitionKey, long sequence) {
        Partition partition = partition(partitionKey);
        if (sequence < partition.nextExpected || partition.slots.containsKey(sequence)) {
            return Optional.empty();
        }
        partition.slots.put(sequence, new Slot(null, SlotState.DONE));
        return advance(partition);
    }

    public synchronized long nextExpected(String partitionKey) {
        return partition(partitionKey).nextExpected;
    }

    /** Missing sequence numbers that are blocking buffered transactions, per partition. */
    public synchronized Map<String, List<Long>> missingSequences() {
        Map<String, List<Long>> gaps = new TreeMap<>();
        partitions.forEach((key, partition) -> {
            boolean anyWaiting = partition.slots.values().stream().anyMatch(s -> s.state == SlotState.WAITING);
            if (!anyWaiting) {
                return;
            }
            List<Long> missing = new ArrayList<>();
            for (long seq = partition.nextExpected; seq < partition.slots.lastKey() && missing.size() < MAX_REPORTED_GAPS; seq++) {
                if (!partition.slots.containsKey(seq)) {
                    missing.add(seq);
                }
            }
            if (!missing.isEmpty()) {
                gaps.put(key, missing);
            }
        });
        return gaps;
    }

    private Optional<String> advance(Partition partition) {
        while (true) {
            Slot next = partition.slots.get(partition.nextExpected);
            if (next == null || next.state == SlotState.IN_FLIGHT) {
                return Optional.empty();
            }
            if (next.state == SlotState.WAITING) {
                next.state = SlotState.IN_FLIGHT;
                return Optional.of(next.transactionId);
            }
            partition.slots.remove(partition.nextExpected);
            partition.nextExpected++;
        }
    }

    private Partition partition(String key) {
        return partitions.computeIfAbsent(key, k -> new Partition(policy.firstSequence()));
    }
}

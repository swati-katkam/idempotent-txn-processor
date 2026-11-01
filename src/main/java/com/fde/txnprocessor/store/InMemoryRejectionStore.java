package com.fde.txnprocessor.store;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class InMemoryRejectionStore implements RejectionStore {

    private final List<RejectedRecord> records = new CopyOnWriteArrayList<>();

    @Override
    public void add(RejectedRecord record) {
        records.add(record);
    }

    @Override
    public List<RejectedRecord> findAll() {
        return List.copyOf(records);
    }
}

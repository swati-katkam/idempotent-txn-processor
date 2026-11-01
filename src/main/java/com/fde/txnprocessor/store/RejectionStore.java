package com.fde.txnprocessor.store;

import java.util.List;

/** Port for the dead-letter store holding requests that never became a transaction. */
public interface RejectionStore {

    void add(RejectedRecord record);

    List<RejectedRecord> findAll();
}

package com.fde.txnprocessor.ingest;

import com.fde.txnprocessor.domain.RawTransactionRequest;

import java.util.List;

/**
 * Port for any input: file, queue consumer, polling a DB table, or a buffer fed by an HTTP endpoint.
 * Sources deliver raw records and make no business decisions.
 */
public interface TransactionSource extends AutoCloseable {

    /** Returns up to {@code maxRecords} records; an empty list means nothing is available right now. */
    List<RawTransactionRequest> poll(int maxRecords);

    /** True when the source will never produce more records (end of file, closed queue). */
    boolean isExhausted();

    @Override
    default void close() {
    }
}

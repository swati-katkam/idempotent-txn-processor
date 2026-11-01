package com.fde.txnprocessor.domain;

/**
 * An unvalidated request exactly as delivered by an input source. Every field is a string so that
 * sources never have to make business decisions; parsing and validation belong to the core.
 *
 * @param sourceRef       where the record came from (e.g. {@code transactions.csv:7}), for traceability
 * @param malformedReason set by a source when the record could not even be split into fields
 */
public record RawTransactionRequest(
        String sourceRef,
        String transactionId,
        String requestId,
        String accountId,
        String transactionType,
        String amount,
        String currency,
        String sequenceNumber,
        String eventTime,
        String originalTransactionId,
        String malformedReason) {

    public static RawTransactionRequest malformed(String sourceRef, String reason) {
        return new RawTransactionRequest(sourceRef, null, null, null, null, null, null, null, null, null, reason);
    }
}

package com.fde.txnprocessor.store;

import java.time.Instant;
import java.util.List;

/**
 * A dead-lettered request. transactionId and requestId are whatever the source delivered and may be
 * null or malformed; reasons are codes only.
 */
public record RejectedRecord(String sourceRef, String transactionId, String requestId, List<String> reasons,
                             Instant rejectedAt) {
}

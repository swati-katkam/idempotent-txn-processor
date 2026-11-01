package com.fde.txnprocessor.domain;

import java.time.Instant;

/** One entry in a transaction's audit trail. {@code from} is null for the initial RECEIVED entry. */
public record StatusChange(TransactionStatus from, TransactionStatus to, String reason, Instant at) {
}

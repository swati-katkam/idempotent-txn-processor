package com.fde.txnprocessor.domain;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

/** A validated transaction request. Instances only exist after passing {@code TransactionValidator}. */
public record TransactionRequest(
        String transactionId,
        String requestId,
        String accountId,
        TransactionType type,
        BigDecimal amount,
        String currency,
        long sequenceNumber,
        Instant eventTime,
        String originalTransactionId,
        String sourceRef) {

    public TransactionRequest {
        Objects.requireNonNull(transactionId, "transactionId");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(currency, "currency");
    }

    /**
     * SHA-256 of the fields that define the business meaning of the transaction. requestId,
     * eventTime and sourceRef are excluded on purpose: a retry of the same transaction may carry a
     * new requestId or a new delivery time and must still be recognised as the same transaction.
     * Hashing keeps account data out of the dedup index.
     */
    public String businessFingerprint() {
        String canonical = String.join("|",
                transactionId,
                accountId,
                type.name(),
                amount.stripTrailingZeros().toPlainString(),
                currency,
                Long.toString(sequenceNumber),
                Objects.toString(originalTransactionId, ""));
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}

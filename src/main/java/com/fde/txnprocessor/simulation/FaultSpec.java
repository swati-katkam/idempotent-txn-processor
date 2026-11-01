package com.fde.txnprocessor.simulation;

import java.util.Locale;

/**
 * Describes a simulated transient failure: the first {@code failures} posting calls for
 * {@code transactionId} fail. Text form: {@code TXN-2005:1:BEFORE_COMMIT} (mode optional).
 */
public record FaultSpec(String transactionId, int failures, Mode mode) {

    public enum Mode {
        /** The downstream call fails and nothing is written (e.g. connection refused). */
        BEFORE_COMMIT,
        /** The downstream commits but the acknowledgement is lost (e.g. read timeout). */
        AFTER_COMMIT
    }

    public FaultSpec {
        if (transactionId == null || transactionId.isBlank()) {
            throw new IllegalArgumentException("Fault transactionId is required");
        }
        if (failures < 1) {
            throw new IllegalArgumentException("Fault failure count must be >= 1");
        }
        if (mode == null) {
            mode = Mode.BEFORE_COMMIT;
        }
    }

    public static FaultSpec parse(String text) {
        String[] parts = text.trim().split(":");
        if (parts.length < 2 || parts.length > 3) {
            throw new IllegalArgumentException("Fault must look like TXN-ID:COUNT[:BEFORE_COMMIT|AFTER_COMMIT]: " + text);
        }
        int failures;
        try {
            failures = Integer.parseInt(parts[1].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Fault count is not a number: " + text, e);
        }
        Mode mode = parts.length == 3 ? Mode.valueOf(parts[2].trim().toUpperCase(Locale.ROOT)) : Mode.BEFORE_COMMIT;
        return new FaultSpec(parts[0].trim(), failures, mode);
    }

    FaultSpec decrement() {
        return new FaultSpec(transactionId, failures - 1, mode);
    }
}

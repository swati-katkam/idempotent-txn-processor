package com.fde.txnprocessor.retry;

import java.time.Duration;

/**
 * Bounded exponential backoff.
 *
 * @param maxAttempts total processing attempts including the first one
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, double multiplier, Duration maxBackoff) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (initialBackoff.isNegative() || maxBackoff.isNegative() || multiplier < 1.0) {
            throw new IllegalArgumentException("backoff must be non-negative and multiplier >= 1");
        }
    }

    public boolean canRetry(int attemptsMade) {
        return attemptsMade < maxAttempts;
    }

    /** Delay before the next attempt, after {@code attemptsMade} attempts have failed. */
    public Duration backoffAfter(int attemptsMade) {
        double factor = Math.pow(multiplier, Math.max(0, attemptsMade - 1));
        double millis = Math.min(maxBackoff.toMillis(), initialBackoff.toMillis() * factor);
        return Duration.ofMillis((long) millis);
    }
}

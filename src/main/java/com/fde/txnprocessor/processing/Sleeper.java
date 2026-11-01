package com.fde.txnprocessor.processing;

import java.time.Duration;

/** Abstraction over waiting, so tests can advance a fake clock instead of sleeping. */
@FunctionalInterface
public interface Sleeper {

    void sleep(Duration duration) throws InterruptedException;

    static Sleeper system() {
        return duration -> Thread.sleep(Math.max(0, duration.toMillis()));
    }
}

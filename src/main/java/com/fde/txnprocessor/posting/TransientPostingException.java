package com.fde.txnprocessor.posting;

/** A failure that may succeed if retried (timeout, connection reset, 503, lock contention). */
public class TransientPostingException extends RuntimeException {

    public TransientPostingException(String message) {
        super(message);
    }
}

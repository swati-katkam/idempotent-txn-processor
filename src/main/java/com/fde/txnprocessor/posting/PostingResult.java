package com.fde.txnprocessor.posting;

public enum PostingResult {
    /** The entry was written by this call. */
    APPLIED,
    /** An identical entry already existed; nothing was changed (idempotent replay). */
    ALREADY_APPLIED
}

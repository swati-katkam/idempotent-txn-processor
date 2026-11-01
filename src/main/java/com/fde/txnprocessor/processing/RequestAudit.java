package com.fde.txnprocessor.processing;

/** What the processor decided about one incoming request at the moment it arrived. */
public record RequestAudit(String sourceRef, String transactionId, String requestId, Decision decision,
                           String detail) {

    public enum Decision {
        /** Became (or matched nothing and created) a transaction; its final status lives in the store. */
        ACCEPTED,
        DUPLICATE,
        REJECTED
    }
}

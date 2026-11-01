package com.fde.txnprocessor.posting;

/**
 * Port to the system of record that applies a transaction (a ledger, core banking system or
 * payment provider).
 *
 * <p>Contract: implementations must be idempotent on {@code transactionId}. Posting the same entry
 * twice returns {@link PostingResult#ALREADY_APPLIED} and changes nothing. This is what makes
 * retries safe when the outcome of a previous attempt is unknown (for example a timeout after the
 * downstream committed).
 *
 * @throws TransientPostingException for failures worth retrying
 * @throws BusinessRuleViolation     for deterministic rejections that a retry cannot fix
 */
public interface PostingGateway {

    PostingResult post(LedgerEntry entry);
}

package com.fde.txnprocessor.observability;

/** Masks customer identifiers before they reach logs or reports. */
public final class Masking {

    private static final int VISIBLE_SUFFIX = 3;

    private Masking() {
    }

    /** {@code ACC-201} becomes {@code ****201}. */
    public static String account(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            return "-";
        }
        if (accountId.length() <= VISIBLE_SUFFIX) {
            return "****";
        }
        return "****" + accountId.substring(accountId.length() - VISIBLE_SUFFIX);
    }
}

package com.fde.txnprocessor.validation;

import com.fde.txnprocessor.domain.RawTransactionRequest;
import com.fde.txnprocessor.domain.TransactionRequest;
import com.fde.txnprocessor.domain.TransactionType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.regex.Pattern;

/**
 * Turns a {@link RawTransactionRequest} into a {@link TransactionRequest} or a list of field errors.
 * All fields are checked so a rejected record reports every problem at once.
 */
public final class TransactionValidator {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,63}");
    private static final Pattern CURRENCY_CODE = Pattern.compile("[A-Z]{3}");

    private final BigDecimal maxAmount;

    public TransactionValidator(BigDecimal maxAmount) {
        if (maxAmount.signum() <= 0) {
            throw new IllegalArgumentException("maxAmount must be positive");
        }
        this.maxAmount = maxAmount;
    }

    public ValidationResult validate(RawTransactionRequest raw) {
        if (raw.malformedReason() != null) {
            return new ValidationResult.Invalid(List.of(new ValidationError("record", raw.malformedReason())));
        }
        List<ValidationError> errors = new ArrayList<>();

        String transactionId = identifier("transactionId", raw.transactionId(), true, errors);
        String requestId = identifier("requestId", raw.requestId(), true, errors);
        String accountId = identifier("accountId", raw.accountId(), true, errors);
        TransactionType type = type(raw.transactionType(), errors);
        String currency = currency(raw.currency(), errors);
        BigDecimal amount = amount(raw.amount(), currency, errors);
        Long sequence = sequence(raw.sequenceNumber(), errors);
        Instant eventTime = eventTime(raw.eventTime(), errors);
        String original = identifier("originalTransactionId", raw.originalTransactionId(),
                type == TransactionType.REVERSAL, errors);

        if (type != null && type != TransactionType.REVERSAL && original != null) {
            errors.add(new ValidationError("originalTransactionId", "NOT_ALLOWED"));
        }
        if (original != null && original.equals(transactionId)) {
            errors.add(new ValidationError("originalTransactionId", "SELF_REFERENCE"));
        }

        if (!errors.isEmpty()) {
            return new ValidationResult.Invalid(List.copyOf(errors));
        }
        return new ValidationResult.Valid(new TransactionRequest(transactionId, requestId, accountId, type, amount,
                currency, sequence, eventTime, original, raw.sourceRef()));
    }

    public static boolean isIdentifier(String value) {
        return value != null && IDENTIFIER.matcher(value.trim()).matches();
    }

    /** Lenient sequence parse used to free the ordering slot of a record that failed validation. */
    public static OptionalLong parseSequence(String value) {
        if (isBlank(value)) {
            return OptionalLong.empty();
        }
        try {
            long parsed = Long.parseLong(value.trim());
            return parsed >= 1 ? OptionalLong.of(parsed) : OptionalLong.empty();
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    private static String identifier(String field, String value, boolean required, List<ValidationError> errors) {
        if (isBlank(value)) {
            if (required) {
                errors.add(new ValidationError(field, "REQUIRED"));
            }
            return null;
        }
        String trimmed = value.trim();
        if (!IDENTIFIER.matcher(trimmed).matches()) {
            errors.add(new ValidationError(field, "INVALID_FORMAT"));
            return null;
        }
        return trimmed;
    }

    private static TransactionType type(String value, List<ValidationError> errors) {
        if (isBlank(value)) {
            errors.add(new ValidationError("transactionType", "REQUIRED"));
            return null;
        }
        try {
            return TransactionType.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            errors.add(new ValidationError("transactionType", "UNSUPPORTED_VALUE"));
            return null;
        }
    }

    private static String currency(String value, List<ValidationError> errors) {
        if (isBlank(value)) {
            errors.add(new ValidationError("currency", "REQUIRED"));
            return null;
        }
        String code = value.trim().toUpperCase(Locale.ROOT);
        if (!CURRENCY_CODE.matcher(code).matches()) {
            errors.add(new ValidationError("currency", "INVALID_FORMAT"));
            return null;
        }
        try {
            Currency.getInstance(code);
            return code;
        } catch (IllegalArgumentException e) {
            errors.add(new ValidationError("currency", "UNSUPPORTED_VALUE"));
            return null;
        }
    }

    private BigDecimal amount(String value, String currency, List<ValidationError> errors) {
        if (isBlank(value)) {
            errors.add(new ValidationError("amount", "REQUIRED"));
            return null;
        }
        BigDecimal amount;
        try {
            amount = new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            errors.add(new ValidationError("amount", "INVALID_FORMAT"));
            return null;
        }
        if (amount.signum() <= 0) {
            errors.add(new ValidationError("amount", "MUST_BE_POSITIVE"));
            return null;
        }
        if (amount.compareTo(maxAmount) > 0) {
            errors.add(new ValidationError("amount", "EXCEEDS_LIMIT"));
            return null;
        }
        if (currency != null) {
            int fractionDigits = Currency.getInstance(currency).getDefaultFractionDigits();
            if (fractionDigits >= 0 && amount.stripTrailingZeros().scale() > fractionDigits) {
                errors.add(new ValidationError("amount", "TOO_MANY_DECIMALS"));
                return null;
            }
        }
        return amount;
    }

    private static Long sequence(String value, List<ValidationError> errors) {
        if (isBlank(value)) {
            errors.add(new ValidationError("sequenceNumber", "REQUIRED"));
            return null;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed < 1) {
                errors.add(new ValidationError("sequenceNumber", "MUST_BE_POSITIVE"));
                return null;
            }
            return parsed;
        } catch (NumberFormatException e) {
            errors.add(new ValidationError("sequenceNumber", "INVALID_FORMAT"));
            return null;
        }
    }

    private static Instant eventTime(String value, List<ValidationError> errors) {
        if (isBlank(value)) {
            return null;
        }
        try {
            return Instant.parse(value.trim());
        } catch (DateTimeParseException e) {
            errors.add(new ValidationError("eventTime", "INVALID_FORMAT"));
            return null;
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}

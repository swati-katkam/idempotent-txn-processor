package com.fde.txnprocessor;

import com.fde.txnprocessor.domain.RawTransactionRequest;
import com.fde.txnprocessor.support.Harness;
import com.fde.txnprocessor.support.Raw;
import com.fde.txnprocessor.validation.TransactionValidator;
import com.fde.txnprocessor.validation.ValidationError;
import com.fde.txnprocessor.validation.ValidationResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidationTest {

    private final TransactionValidator validator = new TransactionValidator(new BigDecimal("1000000"));

    static Stream<Arguments> invalidRequests() {
        return Stream.of(
                Arguments.of(Raw.txn(null, "REQ-1", 1), "transactionId:REQUIRED"),
                Arguments.of(Raw.txn("TXN-1", " ", 1), "requestId:REQUIRED"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).account(null), "accountId:REQUIRED"),
                Arguments.of(Raw.txn("TXN 1;DROP", "REQ-1", 1), "transactionId:INVALID_FORMAT"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).type("TRANSFER"), "transactionType:UNSUPPORTED_VALUE"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).amount("-10"), "amount:MUST_BE_POSITIVE"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).amount("0"), "amount:MUST_BE_POSITIVE"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).amount("12abc"), "amount:INVALID_FORMAT"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).amount("10.001"), "amount:TOO_MANY_DECIMALS"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).amount("1000000.01"), "amount:EXCEEDS_LIMIT"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).currency("XYZ"), "currency:UNSUPPORTED_VALUE"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).currency("US"), "currency:INVALID_FORMAT"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).sequence("abc"), "sequenceNumber:INVALID_FORMAT"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).sequence("0"), "sequenceNumber:MUST_BE_POSITIVE"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).eventTime("yesterday"), "eventTime:INVALID_FORMAT"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).type("REVERSAL"), "originalTransactionId:REQUIRED"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).original("TXN-0"), "originalTransactionId:NOT_ALLOWED"),
                Arguments.of(Raw.txn("TXN-1", "REQ-1", 1).reversal("100", "TXN-1"), "originalTransactionId:SELF_REFERENCE"));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("invalidRequests")
    void rejectsInvalidField(Raw request, String expectedError) {
        ValidationResult result = validator.validate(request.build());

        ValidationResult.Invalid invalid = assertInstanceOf(ValidationResult.Invalid.class, result);
        assertTrue(invalid.errors().stream().map(ValidationError::toString).anyMatch(expectedError::equals),
                () -> "expected " + expectedError + " in " + invalid.errors());
    }

    @Test
    void reportsEveryProblemAtOnce() {
        ValidationResult result = validator.validate(
                Raw.txn("TXN-1", null, 1).amount("-1").currency("??").build());

        assertEquals(3, ((ValidationResult.Invalid) result).errors().size());
    }

    @Test
    void acceptsAndNormalisesAValidRequest() {
        ValidationResult result = validator.validate(
                Raw.txn(" TXN-1 ", "REQ-1", 7).type("credit").currency("usd").amount("10.50")
                        .eventTime("2026-11-01T09:00:00Z").build());

        ValidationResult.Valid valid = assertInstanceOf(ValidationResult.Valid.class, result);
        assertEquals("TXN-1", valid.request().transactionId());
        assertEquals("USD", valid.request().currency());
        assertEquals(7, valid.request().sequenceNumber());
    }

    @Test
    void invalidRecordsAreIsolatedAndTheBatchContinues() {
        Harness h = new Harness();
        h.module.newWorker(com.fde.txnprocessor.ingest.InMemoryTransactionSource.completedWith(List.of(
                Raw.txn("TXN-1", "REQ-1", 1).credit("10").build(),
                RawTransactionRequest.malformed("file.csv:3", "COLUMN_COUNT_MISMATCH"),
                Raw.txn("TXN-2", "REQ-2", 2).amount("oops").build(),
                Raw.txn("TXN-3", "REQ-3", 3).credit("5").build())), h.clock::advance).run();

        assertEquals(List.of("TXN-1", "TXN-3"), h.postedTransactionIds());
        assertEquals(2, h.module.rejections().findAll().size());
        assertEquals(List.of("record:COLUMN_COUNT_MISMATCH"), h.module.rejections().findAll().get(0).reasons());
    }

    @Test
    void rejectionReasonsNeverEchoTheRejectedValue() {
        Harness h = new Harness().run(Raw.txn("TXN-1", "REQ-1", 1).amount("4111111111111111x"));

        String reasons = String.join(",", h.module.rejections().findAll().get(0).reasons());
        assertTrue(!reasons.contains("4111"), reasons);
    }
}

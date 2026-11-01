package com.fde.txnprocessor.validation;

import com.fde.txnprocessor.domain.TransactionRequest;

import java.util.List;

public sealed interface ValidationResult permits ValidationResult.Valid, ValidationResult.Invalid {

    record Valid(TransactionRequest request) implements ValidationResult {
    }

    record Invalid(List<ValidationError> errors) implements ValidationResult {
    }
}

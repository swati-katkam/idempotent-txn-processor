package com.fde.txnprocessor.ingest;

import com.fde.txnprocessor.domain.RawTransactionRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Streams a CSV file with a header row. Columns are matched by name, so order does not matter.
 * Blank lines and lines starting with {@code #} are ignored. A row with the wrong number of columns
 * is passed on as malformed instead of stopping the file. Quoted fields are not supported, since no
 * field in this model needs them.
 */
public final class CsvFileTransactionSource implements TransactionSource {

    static final List<String> REQUIRED_COLUMNS = List.of(
            "transactionId", "requestId", "accountId", "transactionType", "amount", "currency", "sequenceNumber");

    private final BufferedReader reader;
    private final String fileName;
    private final Map<String, Integer> columns = new HashMap<>();
    private int lineNumber;
    private boolean exhausted;

    public CsvFileTransactionSource(Path path) throws IOException {
        this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
        this.fileName = path.getFileName().toString();
        String header = nextContentLine();
        if (header == null) {
            exhausted = true;
            return;
        }
        String[] names = header.replace("﻿", "").split(",", -1);
        for (int i = 0; i < names.length; i++) {
            columns.put(names[i].trim(), i);
        }
        List<String> missing = REQUIRED_COLUMNS.stream().filter(c -> !columns.containsKey(c)).toList();
        if (!missing.isEmpty()) {
            reader.close();
            throw new IllegalArgumentException(fileName + " is missing required columns " + missing);
        }
    }

    @Override
    public List<RawTransactionRequest> poll(int maxRecords) {
        List<RawTransactionRequest> batch = new ArrayList<>();
        try {
            while (!exhausted && batch.size() < maxRecords) {
                String line = nextContentLine();
                if (line == null) {
                    exhausted = true;
                    reader.close();
                } else {
                    batch.add(toRaw(line));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading " + fileName, e);
        }
        return batch;
    }

    @Override
    public boolean isExhausted() {
        return exhausted;
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RawTransactionRequest toRaw(String line) {
        String sourceRef = fileName + ":" + lineNumber;
        String[] values = line.split(",", -1);
        if (values.length != columns.size()) {
            return RawTransactionRequest.malformed(sourceRef, "COLUMN_COUNT_MISMATCH");
        }
        return new RawTransactionRequest(sourceRef,
                value(values, "transactionId"),
                value(values, "requestId"),
                value(values, "accountId"),
                value(values, "transactionType"),
                value(values, "amount"),
                value(values, "currency"),
                value(values, "sequenceNumber"),
                value(values, "eventTime"),
                value(values, "originalTransactionId"),
                null);
    }

    private String value(String[] values, String column) {
        Integer index = columns.get(column);
        if (index == null) {
            return null;
        }
        String value = values[index].trim();
        return value.isEmpty() ? null : value;
    }

    private String nextContentLine() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                return line;
            }
        }
        return null;
    }
}

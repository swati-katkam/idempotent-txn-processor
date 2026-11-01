package com.fde.txnprocessor;

import com.fde.txnprocessor.domain.RawTransactionRequest;
import com.fde.txnprocessor.domain.TransactionStatus;
import com.fde.txnprocessor.ingest.CsvFileTransactionSource;
import com.fde.txnprocessor.support.Harness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvSourceTest {

    @TempDir
    Path dir;

    @Test
    void readsRowsByHeaderNameAndFlagsMalformedRows() throws IOException {
        Path file = dir.resolve("in.csv");
        Files.writeString(file, String.join("\n",
                "# comment",
                "sequenceNumber,transactionId,requestId,accountId,transactionType,amount,currency",
                "1,TXN-1,REQ-1,ACC-1,CREDIT,10,USD",
                "",
                "2,TXN-2,REQ-2"));

        try (CsvFileTransactionSource source = new CsvFileTransactionSource(file)) {
            List<RawTransactionRequest> rows = source.poll(10);

            assertEquals(2, rows.size());
            assertEquals("TXN-1", rows.get(0).transactionId());
            assertEquals("1", rows.get(0).sequenceNumber());
            assertEquals("in.csv:3", rows.get(0).sourceRef());
            assertNull(rows.get(0).eventTime());
            assertEquals("COLUMN_COUNT_MISMATCH", rows.get(1).malformedReason());
            assertEquals("in.csv:5", rows.get(1).sourceRef());
            assertTrue(source.isExhausted());
        }
    }

    @Test
    void missingRequiredColumnFailsFast() throws IOException {
        Path file = dir.resolve("bad.csv");
        Files.writeString(file, "transactionId,amount\nTXN-1,10\n");

        assertThrows(IllegalArgumentException.class, () -> new CsvFileTransactionSource(file));
    }

    @Test
    void bundledSampleFileProcessesEndToEnd() throws IOException {
        Harness h = Harness.withFaults("TXN-2005:1");
        try (CsvFileTransactionSource source = new CsvFileTransactionSource(Path.of("samples", "transactions.csv"))) {
            h.module.newWorker(source, h.clock::advance).run();
        }

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-2005"));
        assertEquals(new BigDecimal("85"), h.balance("ACC-201"));
        assertEquals(6, h.module.report().requestsReceived());
    }

    @Test
    void edgeCaseSampleFileProducesTheDocumentedOutcomes() throws IOException {
        Harness h = Harness.withFaults("TXN-3003:5:BEFORE_COMMIT", "TXN-3004:1:AFTER_COMMIT");
        try (CsvFileTransactionSource source =
                     new CsvFileTransactionSource(Path.of("samples", "transactions-edge-cases.csv"))) {
            h.module.newWorker(source, h.clock::advance).run();
        }

        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-3001"));
        assertEquals("INSUFFICIENT_FUNDS", h.record("TXN-3002").reason());
        assertEquals(TransactionStatus.FAILED, h.status("TXN-3003"));
        assertEquals("ALREADY_APPLIED", h.record("TXN-3004").reason());
        assertEquals(TransactionStatus.PROCESSED, h.status("TXN-3005"));
        assertEquals(TransactionStatus.PENDING, h.status("TXN-3007"));
        assertEquals(List.of(6L), h.module.report().missingSequences().get("GLOBAL"));
        assertEquals(new BigDecimal("200.00"), h.balance("ACC-301"));
        assertEquals(3, h.module.rejections().findAll().size());
    }
}

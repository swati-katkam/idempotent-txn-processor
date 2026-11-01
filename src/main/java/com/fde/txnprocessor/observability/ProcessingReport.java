package com.fde.txnprocessor.observability;

import com.fde.txnprocessor.store.RejectedRecord;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Operator-facing summary of a processing run. Account identifiers are masked, and amounts appear
 * only as aggregated balances.
 *
 * @param requestsByFinalStatus where each received request ended up; sums to the number received
 * @param transactionsByStatus  status of each unique business transaction
 */
public record ProcessingReport(
        Instant generatedAt,
        long requestsReceived,
        Map<String, Long> requestsByFinalStatus,
        Map<String, Long> transactionsByStatus,
        Map<String, Long> counters,
        List<RequestLine> requests,
        List<TransactionLine> transactions,
        List<RejectedRecord> rejected,
        Map<String, List<Long>> missingSequences,
        Map<String, String> balances) {

    public record RequestLine(String sourceRef, String transactionId, String requestId, String decision,
                              String finalStatus, String detail) {
    }

    public record TransactionLine(String transactionId, String account, String type, long sequence, String status,
                                  int attempts, String reason, String lifecycle) {
    }

    public String toJson() {
        return Json.write(this);
    }

    public String toText() {
        StringBuilder out = new StringBuilder();
        String rule = "=".repeat(100);
        out.append(rule).append('\n')
                .append(" TRANSACTION PROCESSING SUMMARY  (generated ").append(generatedAt).append(")\n")
                .append(rule).append('\n');
        out.append(String.format(" Requests received : %d%n", requestsReceived));
        out.append(" Requests by final status : ").append(inline(requestsByFinalStatus)).append('\n');
        out.append(" Unique transactions by status : ").append(inline(transactionsByStatus)).append('\n');
        out.append(" Event counters : ").append(inline(counters)).append('\n');

        out.append("\n-- Requests (arrival order) ").append("-".repeat(72)).append('\n');
        out.append(String.format(" %-32s %-10s %-12s %-9s %-13s %s%n",
                "source", "txnId", "requestId", "decision", "finalStatus", "detail"));
        for (RequestLine r : requests) {
            out.append(String.format(" %-32s %-10s %-12s %-9s %-13s %s%n", r.sourceRef(), dash(r.transactionId()),
                    dash(r.requestId()), r.decision(), r.finalStatus(), dash(r.detail())));
        }

        out.append("\n-- Transactions (sequence order) ").append("-".repeat(66)).append('\n');
        out.append(String.format(" %-10s %-8s %-9s %4s %-13s %4s  %-28s %s%n",
                "txnId", "account", "type", "seq", "status", "att", "reason", "lifecycle"));
        for (TransactionLine t : transactions) {
            out.append(String.format(" %-10s %-8s %-9s %4d %-13s %4d  %-28s %s%n", t.transactionId(), t.account(),
                    t.type(), t.sequence(), t.status(), t.attempts(), dash(t.reason()), t.lifecycle()));
        }

        out.append("\n-- Rejected / dead-lettered ").append("-".repeat(72)).append('\n');
        if (rejected.isEmpty()) {
            out.append(" none\n");
        }
        for (RejectedRecord r : rejected) {
            out.append(String.format(" %-32s txnId=%s requestId=%s reasons=%s%n", r.sourceRef(),
                    dash(r.transactionId()), dash(r.requestId()), String.join(",", r.reasons())));
        }

        out.append("\n-- Missing sequence numbers (blocking PENDING transactions) ").append("-".repeat(40)).append('\n');
        out.append(missingSequences.isEmpty() ? " none\n" : " " + missingSequences + "\n");

        out.append("\n-- Balances (masked) ").append("-".repeat(79)).append('\n');
        balances.forEach((account, balance) -> out.append(String.format(" %-16s %s%n", account, balance)));
        out.append(rule).append('\n');
        return out.toString();
    }

    private static String inline(Map<String, Long> counts) {
        return counts.entrySet().stream()
                .filter(e -> e.getValue() > 0)
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("  "));
    }

    private static String dash(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}

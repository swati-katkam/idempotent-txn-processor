package com.fde.txnprocessor.observability;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/** Single-line log format: {@code <instant> <level> logger=<name> thread=<id> <message>}. */
public final class KeyValueFormatter extends Formatter {

    @Override
    public String format(LogRecord record) {
        String loggerName = record.getLoggerName() == null ? "-" : record.getLoggerName();
        String shortName = loggerName.substring(loggerName.lastIndexOf('.') + 1);
        StringBuilder line = new StringBuilder()
                .append(record.getInstant()).append(' ')
                .append(String.format("%-7s", record.getLevel().getName())).append(' ')
                .append("logger=").append(shortName).append(' ')
                .append("thread=").append(record.getLongThreadID()).append(' ')
                .append(record.getMessage());
        if (record.getThrown() != null) {
            // exception class only: messages from downstream systems can echo customer data
            line.append(" exception=").append(record.getThrown().getClass().getName());
        }
        return line.append(System.lineSeparator()).toString();
    }
}

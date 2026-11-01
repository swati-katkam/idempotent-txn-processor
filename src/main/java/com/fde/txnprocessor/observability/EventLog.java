package com.fde.txnprocessor.observability;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Structured logging on top of java.util.logging. Every line is
 * {@code event=NAME key=value key=value}, which is greppable and parses directly in Splunk, ELK or
 * CloudWatch Logs Insights. Values are sanitised so input data cannot forge log lines.
 */
public final class EventLog {

    private final Logger logger;

    private EventLog(Logger logger) {
        this.logger = logger;
    }

    public static EventLog forClass(Class<?> type) {
        return new EventLog(Logger.getLogger(type.getName()));
    }

    public void info(String event, Object... keyValues) {
        log(Level.INFO, event, keyValues);
    }

    public void warn(String event, Object... keyValues) {
        log(Level.WARNING, event, keyValues);
    }

    public void error(String event, Object... keyValues) {
        log(Level.SEVERE, event, keyValues);
    }

    private void log(Level level, String event, Object... keyValues) {
        if (!logger.isLoggable(level)) {
            return;
        }
        StringBuilder line = new StringBuilder("event=").append(event);
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            line.append(' ').append(keyValues[i]).append('=').append(format(keyValues[i + 1]));
        }
        logger.log(level, line.toString());
    }

    static String format(Object value) {
        if (value == null) {
            return "-";
        }
        String text = value.toString().replaceAll("\\p{Cntrl}", "?");
        if (text.isEmpty() || text.indexOf(' ') >= 0 || text.indexOf('=') >= 0 || text.indexOf('"') >= 0) {
            return '"' + text.replace("\"", "\\\"") + '"';
        }
        return text;
    }
}

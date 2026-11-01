package com.fde.txnprocessor.config;

import com.fde.txnprocessor.ordering.OrderingPolicy;
import com.fde.txnprocessor.retry.RetryPolicy;
import com.fde.txnprocessor.simulation.FaultSpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Runtime configuration. Loaded from {@code application.properties} on the classpath; any key can
 * be overridden with a JVM system property prefixed {@code txn.}, e.g.
 * {@code -Dtxn.retry.maxAttempts=5}. No secrets are needed; a real deployment would inject
 * credentials from the environment or a secret manager, never from this file.
 */
public record ProcessorConfig(
        int batchSize,
        Duration idlePollInterval,
        RetryPolicy retryPolicy,
        OrderingPolicy orderingPolicy,
        BigDecimal maxAmount,
        List<FaultSpec> faults) {

    public static final String OVERRIDE_PREFIX = "txn.";

    public ProcessorConfig {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        faults = List.copyOf(faults);
    }

    public static ProcessorConfig load() {
        Properties properties = new Properties();
        try (InputStream in = ProcessorConfig.class.getResourceAsStream("/application.properties")) {
            if (in != null) {
                properties.load(in);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read application.properties", e);
        }
        System.getProperties().stringPropertyNames().stream()
                .filter(name -> name.startsWith(OVERRIDE_PREFIX))
                .forEach(name -> properties.setProperty(name.substring(OVERRIDE_PREFIX.length()), System.getProperty(name)));
        return fromProperties(properties);
    }

    public static ProcessorConfig fromProperties(Properties p) {
        RetryPolicy retry = new RetryPolicy(
                intValue(p, "retry.maxAttempts", 3),
                Duration.ofMillis(longValue(p, "retry.initialBackoffMillis", 100)),
                Double.parseDouble(p.getProperty("retry.multiplier", "2.0")),
                Duration.ofMillis(longValue(p, "retry.maxBackoffMillis", 2_000)));
        OrderingPolicy ordering = new OrderingPolicy(
                OrderingPolicy.Scope.valueOf(p.getProperty("ordering.scope", "GLOBAL").trim().toUpperCase(Locale.ROOT)),
                longValue(p, "ordering.firstSequence", 1),
                Boolean.parseBoolean(p.getProperty("ordering.rejectedRecordsReleaseSequence", "true").trim()));
        return new ProcessorConfig(
                intValue(p, "worker.batchSize", 50),
                Duration.ofMillis(longValue(p, "worker.idlePollMillis", 200)),
                retry,
                ordering,
                new BigDecimal(p.getProperty("validation.maxAmount", "1000000").trim()),
                parseFaults(p.getProperty("simulation.faults", "")));
    }

    public static ProcessorConfig defaults() {
        return fromProperties(new Properties());
    }

    public ProcessorConfig withFaults(List<FaultSpec> newFaults) {
        return new ProcessorConfig(batchSize, idlePollInterval, retryPolicy, orderingPolicy, maxAmount, newFaults);
    }

    public ProcessorConfig withRetryPolicy(RetryPolicy policy) {
        return new ProcessorConfig(batchSize, idlePollInterval, policy, orderingPolicy, maxAmount, faults);
    }

    public ProcessorConfig withOrderingPolicy(OrderingPolicy policy) {
        return new ProcessorConfig(batchSize, idlePollInterval, retryPolicy, policy, maxAmount, faults);
    }

    public static List<FaultSpec> parseFaults(String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        return Arrays.stream(text.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(FaultSpec::parse)
                .toList();
    }

    private static int intValue(Properties p, String key, int fallback) {
        return Integer.parseInt(p.getProperty(key, Integer.toString(fallback)).trim());
    }

    private static long longValue(Properties p, String key, long fallback) {
        return Long.parseLong(p.getProperty(key, Long.toString(fallback)).trim());
    }
}

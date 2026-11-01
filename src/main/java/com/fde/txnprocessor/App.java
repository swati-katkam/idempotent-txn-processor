package com.fde.txnprocessor;

import com.fde.txnprocessor.config.ProcessorConfig;
import com.fde.txnprocessor.ingest.CsvFileTransactionSource;
import com.fde.txnprocessor.observability.KeyValueFormatter;
import com.fde.txnprocessor.observability.ProcessingReport;
import com.fde.txnprocessor.processing.Sleeper;
import com.fde.txnprocessor.processing.TransactionWorker;
import com.fde.txnprocessor.simulation.FaultSpec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * CLI entry point. Runs the worker on a background thread over a CSV file, then prints the
 * processing summary (stdout) and writes a JSON report. Logs go to stderr.
 *
 * <p>Exit codes: 0 means every request was resolved; 2 means the run finished but some transactions
 * are FAILED or still PENDING and need attention; 1 means a usage or startup error.
 */
public final class App {

    private static final String USAGE = String.join(System.lineSeparator(),
            "Usage: java -jar target/txn-processor.jar [input.csv] [options]",
            "  input.csv              CSV to process (default: samples/transactions.csv)",
            "  --report <path>        JSON report path (default: out/processing-report.json)",
            "  --fault <spec>         simulate transient failures, repeatable; replaces simulation.faults",
            "                         spec = TXN-ID:COUNT[:BEFORE_COMMIT|AFTER_COMMIT]",
            "  --no-faults            disable all simulated failures",
            "  -Dtxn.<key>=<value>    override any application.properties key (JVM option)");

    private App() {
    }

    public static void main(String[] args) throws Exception {
        configureLogging();
        Path input = Path.of("samples", "transactions.csv");
        Path reportPath = Path.of("out", "processing-report.json");
        List<FaultSpec> faults = new ArrayList<>();
        boolean faultsOverridden = false;

        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--report" -> reportPath = Path.of(requireValue(args, ++i));
                    case "--fault" -> {
                        faults.add(FaultSpec.parse(requireValue(args, ++i)));
                        faultsOverridden = true;
                    }
                    case "--no-faults" -> faultsOverridden = true;
                    case "-h", "--help" -> {
                        System.out.println(USAGE);
                        return;
                    }
                    default -> {
                        if (args[i].startsWith("--")) {
                            throw new IllegalArgumentException("Unknown option " + args[i]);
                        }
                        input = Path.of(args[i]);
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println(USAGE);
            System.exit(1);
        }
        if (!Files.isReadable(input)) {
            System.err.println("ERROR: cannot read input file " + input);
            System.exit(1);
        }

        ProcessorConfig config = ProcessorConfig.load();
        if (faultsOverridden) {
            config = config.withFaults(faults);
        }
        TransactionProcessingModule module = TransactionProcessingModule.create(config, Clock.systemUTC());

        ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "txn-worker-1"));
        try (CsvFileTransactionSource source = new CsvFileTransactionSource(input)) {
            TransactionWorker worker = module.newWorker(source, Sleeper.system());
            Thread shutdownHook = new Thread(worker::stop, "txn-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            Future<?> run = executor.submit(worker);
            run.get();
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } finally {
            executor.shutdown();
        }

        ProcessingReport report = module.report();
        System.out.print(report.toText());
        if (reportPath.toAbsolutePath().getParent() != null) {
            Files.createDirectories(reportPath.toAbsolutePath().getParent());
        }
        Files.writeString(reportPath, report.toJson());
        System.out.println("JSON report written to " + reportPath);

        boolean needsAttention = report.requestsByFinalStatus().getOrDefault("FAILED", 0L) > 0
                || report.requestsByFinalStatus().getOrDefault("PENDING", 0L) > 0
                || report.requestsByFinalStatus().getOrDefault("RETRY_PENDING", 0L) > 0;
        System.exit(needsAttention ? 2 : 0);
    }

    private static String requireValue(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + args[index - 1]);
        }
        return args[index];
    }

    private static void configureLogging() {
        LogManager.getLogManager().reset();
        Level level = Level.parse(System.getProperty("txn.log.level", "INFO"));
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new KeyValueFormatter());
        handler.setLevel(level);
        Logger root = Logger.getLogger("");
        root.setLevel(level);
        root.addHandler(handler);
    }
}

package evaluator.output;

import evaluator.configuration.JobConfig;
import evaluator.model.AdvisoryRecord;
import evaluator.model.IndicatorRecord;
import evaluator.model.QueryOutput;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;

public final class RunOutput implements AutoCloseable {
    private static final String INDICATOR_HEADER =
            "symbol,window_start,window_end,close,ema38,ema100,closing_timestamp,file_ordinal,source_line";
    private static final String ADVISORY_HEADER =
            "advisory_id,symbol,window_start,window_end,direction";
    private final JobConfig config;
    private final PrintStream console;
    private final String runId = UUID.randomUUID().toString();
    private final Instant startedAt = Instant.now();
    private final Path targetDirectory;
    private final Path stagingDirectory;
    private BufferedWriter indicators;
    private BufferedWriter advisories;
    private long publishedIndicators;
    private long publishedAdvisories;
    private boolean committed;

    public RunOutput(JobConfig config, PrintStream console) throws IOException {
        this.config = config;
        this.console = console;
        targetDirectory = config.outputDirectory();
        if (targetDirectory == null) {
            stagingDirectory = null;
            console.println(INDICATOR_HEADER);
            return;
        }
        if (Files.exists(targetDirectory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Output directory already exists; choose a new path: " + targetDirectory);
        }
        Path parent = targetDirectory.getParent();
        if (parent == null) {
            throw new IOException("Output directory must have a parent: " + targetDirectory);
        }
        Files.createDirectories(parent);
        stagingDirectory = Files.createTempDirectory(parent, "." + targetDirectory.getFileName() + ".tmp-");
        try {
            indicators = Files.newBufferedWriter(stagingDirectory.resolve("indicators.csv"));
            advisories = Files.newBufferedWriter(stagingDirectory.resolve("advisories.csv"));
            indicators.write(INDICATOR_HEADER);
            indicators.newLine();
            advisories.write(ADVISORY_HEADER);
            advisories.newLine();
        } catch (IOException exception) {
            try {
                close();
            } catch (IOException cleanupFailure) {
                exception.addSuppressed(cleanupFailure);
            }
            throw exception;
        }
    }

    public void accept(QueryOutput record) throws IOException {
        if (record.indicator != null && record.advisory == null) {
            IndicatorRecord indicator = record.indicator;
            if (!config.isSubscribed(indicator.symbol)) {
                return;
            }
            publishedIndicators++;
            if (indicators != null) {
                writeIndicator(indicators, indicator);
            } else {
                console.println(indicatorCsv(indicator));
            }
        } else if (record.advisory != null && record.indicator == null) {
            AdvisoryRecord advisory = record.advisory;
            if (!config.isSubscribed(advisory.symbol)) {
                return;
            }
            publishedAdvisories++;
            if (advisories != null) {
                writeAdvisory(advisories, advisory);
            } else {
                console.println("ADVISORY," + advisoryCsv(advisory));
            }
        } else {
            throw new IllegalArgumentException("Query output must contain exactly one record type");
        }
    }

    public long publishedIndicators() {
        return publishedIndicators;
    }

    public long publishedAdvisories() {
        return publishedAdvisories;
    }

    public void finish(Map<String, Object> counters) throws IOException {
        if (stagingDirectory == null) {
            committed = true;
            return;
        }
        try (BufferedWriter metadata = Files.newBufferedWriter(stagingDirectory.resolve("run-metadata.csv"))) {
            metadata.write("key,value");
            metadata.newLine();
            metadataRow(metadata, "run_id", runId);
            metadataRow(metadata, "started_at", startedAt.toString());
            metadataRow(metadata, "status", "completed");
            metadataRow(metadata, "parallelism", Integer.toString(config.parallelism()));
            metadataRow(metadata, "watermark_delay_ms", Long.toString(config.watermarkDelayMs()));
            metadataRow(metadata, "published_indicators", Long.toString(publishedIndicators));
            metadataRow(metadata, "published_advisories", Long.toString(publishedAdvisories));
            metadataRow(metadata, "subscription_mode", config.subscriptions().isEmpty() ? "all" : "allowlist");
            for (String input : config.inputs()) {
                metadataRow(metadata, "input", input);
            }
            for (String symbol : config.subscriptions().stream().sorted(Comparator.naturalOrder()).toList()) {
                metadataRow(metadata, "subscription", symbol);
            }
            for (Map.Entry<String, Object> entry : counters.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()).toList()) {
                metadataRow(metadata, "counter." + entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        closeWriters();
        Files.move(stagingDirectory, targetDirectory, StandardCopyOption.ATOMIC_MOVE);
        committed = true;
    }

    private static void writeIndicator(BufferedWriter writer, IndicatorRecord record) throws IOException {
        writer.write(indicatorCsv(record));
        writer.newLine();
    }

    private static String indicatorCsv(IndicatorRecord record) {
        return String.join(",", csv(record.symbol),
                timestamp(record.windowStart), timestamp(record.windowEnd), Double.toString(record.close),
                Double.toString(record.ema38), Double.toString(record.ema100),
                timestampMicros(record.closingTimestampMicros), Integer.toString(record.fileOrdinal),
                Long.toString(record.sourceLine));
    }

    private static void writeAdvisory(BufferedWriter writer, AdvisoryRecord record) throws IOException {
        writer.write(advisoryCsv(record));
        writer.newLine();
    }

    private static String advisoryCsv(AdvisoryRecord record) {
        return String.join(",", csv(record.advisoryId), csv(record.symbol),
                timestamp(record.windowStart), timestamp(record.windowEnd), csv(record.direction));
    }

    private static void metadataRow(BufferedWriter writer, String key, String value) throws IOException {
        writer.write(csv(key));
        writer.write(',');
        writer.write(csv(value));
        writer.newLine();
    }

    private static String timestamp(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atOffset(ZoneOffset.ofHours(2)).toString();
    }

    private static String timestampMicros(long epochMicros) {
        long seconds = Math.floorDiv(epochMicros, 1_000_000L);
        long nanos = Math.floorMod(epochMicros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos).atOffset(ZoneOffset.ofHours(2)).toString();
    }

    private static String csv(String value) {
        if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0
                || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private void closeWriters() throws IOException {
        IOException failure = null;
        for (BufferedWriter writer : new BufferedWriter[]{indicators, advisories}) {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException exception) {
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
        }
        indicators = null;
        advisories = null;
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            closeWriters();
        } catch (IOException exception) {
            failure = exception;
        }
        if (!committed && stagingDirectory != null) {
            try {
                Files.deleteIfExists(stagingDirectory.resolve("indicators.csv"));
                Files.deleteIfExists(stagingDirectory.resolve("advisories.csv"));
                Files.deleteIfExists(stagingDirectory.resolve("run-metadata.csv"));
                Files.deleteIfExists(stagingDirectory);
            } catch (IOException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}

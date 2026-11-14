package evaluator.output;

import evaluator.configuration.JobConfig;
import evaluator.model.AdvisoryRecord;
import evaluator.model.IndicatorRecord;
import evaluator.model.QueryOutput;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;

/** Writes subscribed indicators and advisories to {@code indicators.csv} and {@code advisories.csv}. */
public final class RunOutput implements AutoCloseable {
    private final JobConfig config;
    private final BufferedWriter indicators;
    private final BufferedWriter advisories;

    public RunOutput(JobConfig config) throws IOException {
        this.config = config;
        Files.createDirectories(config.outputDirectory());
        indicators = Files.newBufferedWriter(config.outputDirectory().resolve("indicators.csv"));
        try {
            advisories = Files.newBufferedWriter(config.outputDirectory().resolve("advisories.csv"));
            indicators.write("symbol,window_start,window_end,close,ema38,ema100,"
                    + "closing_timestamp,file_ordinal,source_line\n");
            advisories.write("symbol,window_start,window_end,direction\n");
        } catch (IOException exception) {
            indicators.close();
            throw exception;
        }
    }

    public void accept(QueryOutput record) throws IOException {
        if (record.indicator != null) {
            IndicatorRecord indicator = record.indicator;
            if (config.isSubscribed(indicator.symbol)) {
                indicators.write(String.join(",", indicator.symbol,
                        timestamp(indicator.windowStart), timestamp(indicator.windowEnd),
                        Double.toString(indicator.close),
                        Double.toString(indicator.ema38), Double.toString(indicator.ema100),
                        timestampMicros(indicator.closingTimestampMicros),
                        Integer.toString(indicator.fileOrdinal), Long.toString(indicator.sourceLine)));
                indicators.newLine();
            }
        } else {
            AdvisoryRecord advisory = record.advisory;
            if (config.isSubscribed(advisory.symbol)) {
                advisories.write(String.join(",", advisory.symbol,
                        timestamp(advisory.windowStart), timestamp(advisory.windowEnd), advisory.direction));
                advisories.newLine();
            }
        }
    }

    private static String timestamp(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atOffset(ZoneOffset.ofHours(2)).toString();
    }

    private static String timestampMicros(long epochMicros) {
        long seconds = Math.floorDiv(epochMicros, 1_000_000L);
        long nanos = Math.floorMod(epochMicros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos).atOffset(ZoneOffset.ofHours(2)).toString();
    }

    @Override
    public void close() throws IOException {
        try (indicators; advisories) {
            // try-with-resources closes both writers, even if one fails.
        }
    }
}

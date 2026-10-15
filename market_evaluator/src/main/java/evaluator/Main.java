package evaluator;

import evaluator.configuration.JobConfig;
import evaluator.model.IndicatorRecord;
import evaluator.processing.QueryOnePipeline;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Main {
    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) { System.exit(exitCode); }
    }

    static int run(String[] args, PrintStream output, PrintStream error) {
        final JobConfig config;
        try {
            config = JobConfig.parse(args);
        } catch (IllegalArgumentException exception) {
            error.println(exception.getMessage());
            error.println("Usage: evaluator.Main [--parallelism N] [--watermark-delay-ms D] <csv-file>...");
            return 2;
        } catch (IOException exception) {
            error.println("Could not read CSV input: " + exception.getMessage());
            return 1;
        }
        try (var runtime = new LocalFlinkRuntime()) {
            var environment = runtime.environment();
            var indicators = QueryOnePipeline.build(environment, config);
            var collector = new DataStream.Collector<IndicatorRecord>();
            indicators.collectAsync(collector);
            var job = environment.executeAsync("MarketEvaluator Query 1");
            try (var results = collector.getOutput()) {
                output.println("symbol,window_start,window_end,close,ema38,ema100,closing_timestamp,file_ordinal,source_line");
                while (results.hasNext()) { printIndicator(output, results.next()); }
                var completed = job.getJobExecutionResult().get();
                var counts = new TreeMap<String, Long>();
                for (String name : List.of("parser_accepted", "accepted", "late", "non_price", "ignored_zero",
                        "rejected_shape", "rejected_identity", "rejected_price", "rejected_event_timestamp", "indicators")) {
                    counts.put(name, completed.getAccumulatorResult(name));
                }
                output.println("Summary: " + counts);
            } catch (Exception exception) {
                try {
                    job.cancel().get();
                } catch (Exception cancellation) {
                    exception.addSuppressed(cancellation);
                }
                throw exception;
            }
            return 0;
        } catch (Exception exception) {
            Throwable cause = exception;
            while (cause.getCause() != null) { cause = cause.getCause(); }
            error.println("Could not process CSV input " + config.inputs() + ": " + cause.getMessage());
            return 1;
        }
    }

    private static String csvField(String value) {
        if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0
                || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private static void printIndicator(PrintStream output, IndicatorRecord record) {
        long seconds = Math.floorDiv(record.closingTimestampMicros, 1_000_000L);
        long nanos = Math.floorMod(record.closingTimestampMicros, 1_000_000L) * 1_000L;
        output.printf("%s,%s,%s,%s,%s,%s,%s,%d,%d%n", csvField(record.symbol),
                Instant.ofEpochMilli(record.windowStart).atOffset(ZoneOffset.ofHours(2)),
                Instant.ofEpochMilli(record.windowEnd).atOffset(ZoneOffset.ofHours(2)),
                Double.toString(record.close), Double.toString(record.ema38), Double.toString(record.ema100),
                Instant.ofEpochSecond(seconds, nanos).atOffset(ZoneOffset.ofHours(2)),
                record.fileOrdinal, record.sourceLine);
    }
}

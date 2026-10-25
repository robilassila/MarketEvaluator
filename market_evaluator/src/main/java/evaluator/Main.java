package evaluator;

import evaluator.configuration.JobConfig;
import evaluator.model.QueryOutput;
import evaluator.output.RunOutput;
import evaluator.processing.QueryOnePipeline;
import java.io.IOException;
import java.io.PrintStream;
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
            error.println("Usage: evaluator.Main [--parallelism N] [--watermark-delay-ms D]"
                    + " [--subscribe FULL_SYMBOL ...] [--output-dir NEW_DIRECTORY] <csv-file>...");
            return 2;
        } catch (IOException exception) {
            error.println("Could not read CSV input: " + exception.getMessage());
            return 1;
        }
        try (var runOutput = new RunOutput(config, output);
            var runtime = new LocalFlinkRuntime()) {
            var environment = runtime.environment();
            var indicators = QueryOnePipeline.build(environment, config);
            var queryOutput = QueryOnePipeline.queryOutput(indicators);
            var collector = new DataStream.Collector<QueryOutput>();
            queryOutput.collectAsync(collector);
            var job = environment.executeAsync("MarketEvaluator Queries 1 and 2");
            try (var results = collector.getOutput()) {
                while (results.hasNext()) { runOutput.accept(results.next()); }
                var completed = job.getJobExecutionResult().get();
                runOutput.finish(completed.getAllAccumulatorResults());
                var counts = new TreeMap<String, Long>();
                for (String name : List.of("parser_accepted", "accepted", "late", "non_price", "ignored_zero",
                        "rejected_shape", "rejected_identity", "rejected_price", "rejected_event_timestamp",
                        "indicators", "advisories")) {
                    counts.put(name, completed.getAccumulatorResult(name));
                }
                counts.put("published_indicators", runOutput.publishedIndicators());
                counts.put("published_advisories", runOutput.publishedAdvisories());
                output.println("Summary: " + counts);
                if (config.outputDirectory() != null) {
                    output.println("Output directory: " + config.outputDirectory());
                }
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
            error.println("Could not run evaluation for input " + config.inputs() + ": " + cause.getMessage());
            return 1;
        }
    }
}

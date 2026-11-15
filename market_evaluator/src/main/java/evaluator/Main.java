package evaluator;

import evaluator.configuration.JobConfig;
import evaluator.kafka.Topics;
import evaluator.processing.QueryOnePipeline;
import java.io.PrintStream;

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
            error.println(JobConfig.USAGE);
            return 2;
        }
        try (var runtime = new LocalFlinkRuntime()) {
            long events;
            try (Topics topics = new Topics(config.bootstrapServers())) {
                int partitions = topics.partitionCount(config.ticksTopic());
                if (partitions == 0) {
                    error.println("Topic " + config.ticksTopic() + " does not exist; run the producer first");
                    return 1;
                }
                if (config.parallelism() > partitions) {
                    error.println("Parallelism " + config.parallelism() + " exceeds the " + partitions
                            + " partitions of " + config.ticksTopic() + "; extra readers would stay idle");
                    return 2;
                }
                topics.ensure(config.indicatorsTopic(), partitions);
                topics.ensure(config.advisoriesTopic(), partitions);
                events = topics.recordCount(config.ticksTopic());
            }
            var environment = runtime.environment();
            QueryOnePipeline.buildKafkaJob(environment, config);
            var result = environment.executeAsync("MarketEvaluator Queries 1 and 2").getJobExecutionResult().get();
            long millis = result.getNetRuntime();
            // Approximate unless the topic was fully loaded before the job started.
            output.printf("Processed %d events in %.1f s (%.0f events/s) at parallelism %d%n",
                    events, millis / 1000.0, events * 1000.0 / Math.max(1, millis), config.parallelism());
            return 0;
        } catch (Exception exception) {
            Throwable cause = exception;
            while (cause.getCause() != null) { cause = cause.getCause(); }
            error.println("Could not run evaluation: " + cause.getMessage());
            return 1;
        }
    }
}

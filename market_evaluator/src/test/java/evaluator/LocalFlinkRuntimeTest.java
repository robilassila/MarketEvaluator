package evaluator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFlinkRuntimeTest {
    @Test
    void clusterRemainsAvailableUntilIteratorCleanupCompletes() throws Exception {
        var runtime = new LocalFlinkRuntime();
        try (runtime) {
            var environment = runtime.environment();
            environment.setParallelism(1);
            var collector = new DataStream.Collector<Integer>();
            environment.fromData(1, 2, 3).collectAsync(collector);
            var job = environment.executeAsync("Completed job cleanup");
            try (var results = collector.getOutput()) {
                List<Integer> received = new ArrayList<>();
                results.forEachRemaining(received::add);
                assertEquals(List.of(1, 2, 3), received);
                job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
                assertTrue(runtime.isRunning());
                assertEquals(JobStatus.FINISHED, job.getJobStatus().get());
            }
            assertTrue(runtime.isRunning());
            assertEquals(JobStatus.FINISHED, job.getJobStatus().get());
        }
        assertFalse(runtime.isRunning());
    }

    @Test
    void closingAnActiveIteratorStillCancelsItsJob() throws Exception {
        var runtime = new LocalFlinkRuntime();
        try (runtime) {
            var environment = runtime.environment();
            environment.setParallelism(1);
            var collector = new DataStream.Collector<Long>();
            environment.fromSequence(1, Long.MAX_VALUE).collectAsync(collector);
            var job = environment.executeAsync("Active job cleanup");
            try (var results = collector.getOutput()) {
                assertEquals(1L, results.next());
                assertTrue(runtime.isRunning());
            }
            assertThrows(ExecutionException.class,
                    () -> job.getJobExecutionResult().get(30, TimeUnit.SECONDS));
            assertEquals(JobStatus.CANCELED, job.getJobStatus().get());
        }
        assertFalse(runtime.isRunning());
    }

    @Test
    void cliWithWarningsEnabledHasNoShutdownException(@TempDir Path temp) throws Exception {
        Path sample = Path.of("../data/sample-positive-prices-08.csv");
        String output = cli(sample, temp.resolve("sample.log"), 0);
        assertTrue(output.contains("indicators=1263"));
        assertTrue(output.contains("accepted=10000"));
        assertTrue(output.contains("Summary:"));
        assertEquals(1263, output.lines().filter(line -> line.matches("^[^,]+,2021-.*")).count());

        Path malformed = temp.resolve("malformed.csv");
        Files.writeString(malformed, "ID,SecType,Date,Time,Last,Trading date,Trading time\n\"unterminated\n");
        String failure = cli(malformed, temp.resolve("failure.log"), 1);
        assertTrue(failure.contains("Could not process CSV input"));
        assertTrue(failure.contains("unterminated quoted field"));
        assertFalse(failure.contains("Summary:"));
    }

    private static String cli(Path input, Path log, int expectedExit) throws Exception {
        Process process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
                "-cp", System.getProperty("java.class.path"), "evaluator.Main", input.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(45, TimeUnit.SECONDS), "CLI did not terminate");
            String output = Files.readString(log);
            assertEquals(expectedExit, process.exitValue(), output);
            assertFalse(output.contains("MiniCluster is not yet running or has already been shut down"), output);
            assertFalse(output.contains("Failed to get job status"), output);
            return output;
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly().waitFor();
            }
        }
    }
}

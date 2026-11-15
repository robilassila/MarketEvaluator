package evaluator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Test;

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
}

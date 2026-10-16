package evaluator;

import java.util.stream.Stream;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Keeps the local cluster alive until result collection and iterator cleanup finish. */
final class LocalFlinkRuntime implements AutoCloseable, PipelineExecutorServiceLoader, PipelineExecutorFactory {
    private static final String TARGET = "managed-local";
    private MiniCluster cluster;

    StreamExecutionEnvironment environment() {
        Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, TARGET);
        return new StreamExecutionEnvironment(this, configuration, getClass().getClassLoader());
    }

    @Override
    public PipelineExecutor getExecutor(Configuration configuration) {
        return (pipeline, executorConfiguration, classLoader) -> {
            if (cluster != null) {
                throw new IllegalStateException("This local runtime supports one job");
            }
            var plan = PipelineExecutorUtils.getStreamGraph(pipeline, executorConfiguration);
            cluster = new MiniCluster(new MiniClusterConfiguration.Builder()
                    .setConfiguration(executorConfiguration)
                    .setNumTaskManagers(1)
                    .setNumSlotsPerTaskManager(plan.getMaximumParallelism())
                    .setCommonBindAddress("localhost")
                    .withRandomPorts()
                    .build());
            cluster.start();
            return cluster.submitJob(plan).thenApply(submission -> new MiniClusterJobClient(
                    submission.getJobID(), cluster, classLoader,
                    MiniClusterJobClient.JobFinalizationBehavior.NOTHING));
        };
    }

    @Override
    public PipelineExecutorFactory getExecutorFactory(Configuration configuration) {
        if (!isCompatibleWith(configuration)) {
            throw new IllegalArgumentException("Expected managed local execution");
        }
        return this;
    }

    @Override public String getName() { return TARGET; }
    @Override public Stream<String> getExecutorNames() { return Stream.of(TARGET); }
    @Override public boolean isCompatibleWith(Configuration configuration) {
        return TARGET.equals(configuration.get(DeploymentOptions.TARGET));
    }

    boolean isRunning() {
        return cluster != null && cluster.isRunning();
    }

    @Override
    public void close() throws Exception {
        if (cluster != null) {
            cluster.closeAsync().get();
        }
    }
}

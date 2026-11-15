package evaluator.kafka;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

public final class Topics implements AutoCloseable {
    private final Admin admin;

    public Topics(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        admin = Admin.create(properties);
    }

    public int partitionCount(String topic) throws InterruptedException, ExecutionException {
        try {
            return describe(topic).partitions().size();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof UnknownTopicOrPartitionException) {
                return 0;
            }
            throw exception;
        }
    }

    public void ensure(String topic, int partitions) throws InterruptedException, ExecutionException {
        try {
            NewTopic definition = new NewTopic(topic, partitions, (short) 1)
                    .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, "-1"));
            admin.createTopics(List.of(definition)).all().get();
            return;
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof TopicExistsException)) {
                throw exception;
            }
        }
        int existing = partitionCount(topic);
        if (existing != partitions) {
            throw new IllegalStateException("Topic " + topic + " has " + existing
                    + " partitions, expected " + partitions);
        }
    }

    public List<TopicPartition> partitions(String topic) throws InterruptedException, ExecutionException {
        return describe(topic).partitions().stream()
                .map(partition -> new TopicPartition(topic, partition.partition()))
                .toList();
    }

    public long recordCount(String topic) throws InterruptedException, ExecutionException {
        List<TopicPartition> partitions = partitions(topic);
        Map<TopicPartition, Long> earliest = offsets(partitions, OffsetSpec.earliest());
        Map<TopicPartition, Long> latest = offsets(partitions, OffsetSpec.latest());
        return partitions.stream().mapToLong(partition -> latest.get(partition) - earliest.get(partition)).sum();
    }

    private Map<TopicPartition, Long> offsets(Collection<TopicPartition> partitions, OffsetSpec spec)
            throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetSpec> request = partitions.stream()
                .collect(Collectors.toMap(partition -> partition, partition -> spec));
        return admin.listOffsets(request).all().get().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    private TopicDescription describe(String topic) throws InterruptedException, ExecutionException {
        return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
    }

    @Override
    public void close() {
        admin.close();
    }
}

package evaluator.output;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Publishes each record as one CSV line keyed by symbol.
 *
 * The Kafka record timestamp is deliberately left unset. The default serializer would copy the
 * Flink element timestamp (the window's last millisecond), but latency measurement needs the
 * producer-assigned CreateTime, i.e. the wall-clock time the job emitted the record.
 */
public final class CsvLineSerializer<T> implements KafkaRecordSerializationSchema<T> {
    public interface Field<T> extends Function {
        String apply(T record);
    }

    private final String topic;
    private final Field<T> symbol;
    private final Field<T> line;

    public CsvLineSerializer(String topic, Field<T> symbol, Field<T> line) {
        this.topic = topic;
        this.symbol = symbol;
        this.line = line;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T record, KafkaSinkContext context, Long timestamp) {
        return new ProducerRecord<>(topic, null, null,
                symbol.apply(record).getBytes(StandardCharsets.UTF_8),
                line.apply(record).getBytes(StandardCharsets.UTF_8));
    }
}

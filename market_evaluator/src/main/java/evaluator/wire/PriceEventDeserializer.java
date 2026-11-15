package evaluator.wire;

import evaluator.model.PriceEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class PriceEventDeserializer implements KafkaRecordDeserializationSchema<PriceEvent> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<PriceEvent> output) {
        output.collect(PriceEventCodec.decode(record.key(), record.value()));
    }

    @Override
    public TypeInformation<PriceEvent> getProducedType() {
        return TypeInformation.of(PriceEvent.class);
    }
}

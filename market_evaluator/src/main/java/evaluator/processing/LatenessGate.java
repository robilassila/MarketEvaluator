package evaluator.processing;

import evaluator.ingestion.SourceMessage;
import evaluator.model.PriceEvent;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/** Uses the preceding global cutoff, independent of downstream watermark delivery. */
public final class LatenessGate extends RichFlatMapFunction<SourceMessage, PriceEvent> {
    private final long delay;
    private boolean seen;
    private long highWater;
    private transient LongCounter parserAccepted, accepted, late, nonPrice, ignoredZero;
    private transient LongCounter shape, identity, price, timestamp;

    public LatenessGate(long delay) { this.delay = delay; }

    @Override public void open(OpenContext context) {
        parserAccepted = counter("parser_accepted");
        accepted = counter("accepted");
        late = counter("late");
        nonPrice = counter("non_price");
        ignoredZero = counter("ignored_zero");
        shape = counter("rejected_shape");
        identity = counter("rejected_identity");
        price = counter("rejected_price");
        timestamp = counter("rejected_event_timestamp");
    }

    private LongCounter counter(String name) {
        LongCounter counter = getRuntimeContext().getLongCounter(name);
        getRuntimeContext().getMetricGroup().gauge(name, counter::getLocalValue);
        return counter;
    }

    @Override public void flatMap(SourceMessage message, Collector<PriceEvent> output) {
        if (message.summary) {
            parserAccepted.add(message.parserAccepted);
            nonPrice.add(message.nonPrice);
            ignoredZero.add(message.ignoredZero);
            shape.add(message.shape);
            identity.add(message.identity);
            price.add(message.price);
            timestamp.add(message.eventTimestamp);
            return;
        }
        PriceEvent event = message.event;
        long eventMillis = event.timestampMillis();
        long windowEnd = WindowTimes.start(eventMillis) + WindowTimes.SIZE_MS;
        if (seen && windowEnd <= WindowTimes.cutoff(highWater, delay)) {
            late.add(1L);
            return;
        }
        accepted.add(1L);
        highWater = seen ? Math.max(highWater, eventMillis) : eventMillis;
        seen = true;
        output.collect(event);
    }
}

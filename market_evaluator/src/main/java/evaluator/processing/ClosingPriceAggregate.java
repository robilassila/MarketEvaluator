package evaluator.processing;

import evaluator.model.PriceEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public final class ClosingPriceAggregate implements AggregateFunction<PriceEvent, PriceEvent, PriceEvent> {
    @Override public PriceEvent createAccumulator() { return new PriceEvent(); }
    @Override public PriceEvent add(PriceEvent event, PriceEvent current) { return later(event, current); }
    @Override public PriceEvent getResult(PriceEvent current) { return current; }
    @Override public PriceEvent merge(PriceEvent left, PriceEvent right) { return later(left, right); }

    private static PriceEvent later(PriceEvent left, PriceEvent right) {
        if (right.symbol == null) { return left; }
        if (left.symbol == null) { return right; }
        int comparison = Long.compare(left.timestampMicros, right.timestampMicros);
        if (comparison == 0) { comparison = Integer.compare(left.fileOrdinal, right.fileOrdinal); }
        if (comparison == 0) { comparison = Long.compare(left.sourceLine, right.sourceLine); }
        return comparison >= 0 ? left : right;
    }
}

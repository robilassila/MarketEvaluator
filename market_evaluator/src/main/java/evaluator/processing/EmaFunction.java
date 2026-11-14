package evaluator.processing;

import evaluator.model.AdvisoryRecord;
import evaluator.model.IndicatorRecord;
import evaluator.model.PriceEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Turns each evaluated window's closing event into EMA38/EMA100 and an optional crossover
 * advisory. The EMAs live in per-symbol state that outlasts individual windows; Flink evaluates
 * a symbol's windows in time order, so each update sees the previous window's EMAs.
 */
public final class EmaFunction extends ProcessWindowFunction<PriceEvent, IndicatorRecord, String, TimeWindow> {
    public static final OutputTag<AdvisoryRecord> ADVISORIES =
            new OutputTag<>("advisories", TypeInformation.of(AdvisoryRecord.class));
    private static final ValueStateDescriptor<Double> EMA38 = new ValueStateDescriptor<>("ema38", Double.class);
    private static final ValueStateDescriptor<Double> EMA100 = new ValueStateDescriptor<>("ema100", Double.class);

    public static double update(double close, double previous, int period) {
        double alpha = 2.0 / (1.0 + period);
        return close * alpha + previous * (1.0 - alpha);
    }

    public static String crossoverDirection(
            double current38, double current100, double previous38, double previous100) {
        if (current38 > current100 && previous38 <= previous100) {
            return "BUY";
        }
        if (current38 < current100 && previous38 >= previous100) {
            return "SELL";
        }
        return null;
    }

    @Override
    public void process(String symbol, Context context, Iterable<PriceEvent> closingEvent,
            Collector<IndicatorRecord> output) throws Exception {
        PriceEvent close = closingEvent.iterator().next();
        ValueState<Double> ema38 = context.globalState().getState(EMA38);
        ValueState<Double> ema100 = context.globalState().getState(EMA100);
        double previous38 = ema38.value() == null ? 0.0 : ema38.value();
        double previous100 = ema100.value() == null ? 0.0 : ema100.value();

        IndicatorRecord indicator = new IndicatorRecord();
        indicator.symbol = symbol;
        indicator.windowStart = context.window().getStart();
        indicator.windowEnd = context.window().getEnd();
        indicator.close = close.price;
        indicator.closingTimestampMicros = close.timestampMicros;
        indicator.fileOrdinal = close.fileOrdinal;
        indicator.sourceLine = close.sourceLine;
        indicator.ema38 = update(close.price, previous38, 38);
        indicator.ema100 = update(close.price, previous100, 100);
        ema38.update(indicator.ema38);
        ema100.update(indicator.ema100);
        output.collect(indicator);

        String direction = crossoverDirection(indicator.ema38, indicator.ema100, previous38, previous100);
        if (direction != null) {
            context.output(ADVISORIES, AdvisoryRecord.create(
                    symbol, indicator.windowStart, indicator.windowEnd, direction));
        }
    }
}

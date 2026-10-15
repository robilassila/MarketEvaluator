package evaluator.processing;

import evaluator.model.IndicatorRecord;
import evaluator.model.WindowClose;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class EmaFunction extends KeyedProcessFunction<String, WindowClose, IndicatorRecord> {
    private transient ValueState<Double> ema38;
    private transient ValueState<Double> ema100;
    private transient ValueState<Long> lastWindow;
    private transient LongCounter emitted;

    @Override public void open(OpenContext context) {
        ema38 = getRuntimeContext().getState(new ValueStateDescriptor<>("ema38", Double.class));
        ema100 = getRuntimeContext().getState(new ValueStateDescriptor<>("ema100", Double.class));
        lastWindow = getRuntimeContext().getState(new ValueStateDescriptor<>("last-window-end", Long.class));
        emitted = getRuntimeContext().getLongCounter("indicators");
        getRuntimeContext().getMetricGroup().gauge("indicators", emitted::getLocalValue);
    }

    public static double update(double close, double previous, int period) {
        double alpha = 2.0 / (1.0 + period);
        return close * alpha + previous * (1.0 - alpha);
    }

    @Override public void processElement(WindowClose close, Context context,
            Collector<IndicatorRecord> output) throws Exception {
        Long last = lastWindow.value();
        if (last != null && close.windowEnd <= last) {
            throw new IllegalStateException("Duplicate or decreasing window for " + close.symbol);
        }
        Double previous38 = ema38.value();
        Double previous100 = ema100.value();
        IndicatorRecord indicator = IndicatorRecord.from(close);
        indicator.ema38 = update(close.close, previous38 == null ? 0.0 : previous38, 38);
        indicator.ema100 = update(close.close, previous100 == null ? 0.0 : previous100, 100);
        ema38.update(indicator.ema38);
        ema100.update(indicator.ema100);
        lastWindow.update(close.windowEnd);
        emitted.add(1L);
        output.collect(indicator);
    }
}

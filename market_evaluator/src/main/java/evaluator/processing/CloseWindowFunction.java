package evaluator.processing;

import evaluator.model.PriceEvent;
import evaluator.model.WindowClose;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class CloseWindowFunction extends ProcessWindowFunction<PriceEvent, WindowClose, String, TimeWindow> {
    @Override public void process(String symbol, Context context, Iterable<PriceEvent> elements,
            Collector<WindowClose> output) {
        PriceEvent event = elements.iterator().next();
        WindowClose close = new WindowClose();
        close.symbol = symbol;
        close.windowStart = context.window().getStart();
        close.windowEnd = context.window().getEnd();
        close.close = event.price;
        close.closingTimestampMicros = event.timestampMicros;
        close.fileOrdinal = event.fileOrdinal;
        close.sourceLine = event.sourceLine;
        output.collect(close);
    }
}

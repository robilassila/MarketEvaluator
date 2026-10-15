package evaluator.model;

public class IndicatorRecord extends WindowClose {
    public double ema38;
    public double ema100;

    public IndicatorRecord() { }

    public static IndicatorRecord from(WindowClose close) {
        IndicatorRecord result = new IndicatorRecord();
        result.symbol = close.symbol;
        result.windowStart = close.windowStart;
        result.windowEnd = close.windowEnd;
        result.close = close.close;
        result.closingTimestampMicros = close.closingTimestampMicros;
        result.fileOrdinal = close.fileOrdinal;
        result.sourceLine = close.sourceLine;
        return result;
    }
}

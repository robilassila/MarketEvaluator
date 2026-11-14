package evaluator.model;

public class IndicatorRecord {
    public String symbol;
    public long windowStart;
    public long windowEnd;
    public double close;
    public long closingTimestampMicros;
    public int fileOrdinal;
    public long sourceLine;
    public double ema38;
    public double ema100;

    public IndicatorRecord() { }
}

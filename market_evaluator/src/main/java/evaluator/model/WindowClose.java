package evaluator.model;

public class WindowClose {
    public String symbol;
    public long windowStart;
    public long windowEnd;
    public double close;
    public long closingTimestampMicros;
    public int fileOrdinal;
    public long sourceLine;

    public WindowClose() { }
}

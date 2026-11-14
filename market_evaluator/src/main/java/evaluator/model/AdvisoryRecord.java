package evaluator.model;

public class AdvisoryRecord {
    public String symbol;
    public long windowStart;
    public long windowEnd;
    public String direction;

    public AdvisoryRecord() { }

    public static AdvisoryRecord create(String symbol, long windowStart, long windowEnd, String direction) {
        AdvisoryRecord result = new AdvisoryRecord();
        result.symbol = symbol;
        result.windowStart = windowStart;
        result.windowEnd = windowEnd;
        result.direction = direction;
        return result;
    }
}

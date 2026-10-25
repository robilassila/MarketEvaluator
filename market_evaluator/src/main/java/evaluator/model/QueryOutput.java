package evaluator.model;

public class QueryOutput {
    public IndicatorRecord indicator;
    public AdvisoryRecord advisory;

    public QueryOutput() { }

    public static QueryOutput indicator(IndicatorRecord indicator) {
        QueryOutput result = new QueryOutput();
        result.indicator = indicator;
        return result;
    }

    public static QueryOutput advisory(AdvisoryRecord advisory) {
        QueryOutput result = new QueryOutput();
        result.advisory = advisory;
        return result;
    }
}

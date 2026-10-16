package evaluator.ingestion;

import evaluator.CsvTradeParser.ParseSummary;
import evaluator.CsvTradeParser.RejectionReason;
import evaluator.model.PriceEvent;

public class SourceMessage {
    public PriceEvent event;
    public boolean summary;
    public long parserAccepted;
    public long nonPrice;
    public long ignoredZero;
    public long shape;
    public long identity;
    public long price;
    public long eventTimestamp;

    public SourceMessage() { }

    public static SourceMessage event(PriceEvent event) {
        SourceMessage message = new SourceMessage();
        message.event = event;
        return message;
    }

    public static SourceMessage summary(ParseSummary counts) {
        SourceMessage message = new SourceMessage();
        message.summary = true;
        message.parserAccepted = counts.acceptedPriceEvents();
        message.nonPrice = counts.nonPrice();
        message.ignoredZero = counts.ignoredZero();
        message.shape = counts.rejected(RejectionReason.SHAPE);
        message.identity = counts.rejected(RejectionReason.IDENTITY);
        message.price = counts.rejected(RejectionReason.PRICE);
        message.eventTimestamp = counts.rejected(RejectionReason.EVENT_TIMESTAMP);
        return message;
    }
}

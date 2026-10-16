package evaluator.model;

import evaluator.CsvTradeParser.TradeEvent;

/** Explicit primitive fields avoid generic serialization of Java time and record types. */
public class PriceEvent {
    public String symbol;
    public String securityType;
    public double price;
    public long timestampMicros;
    public int fileOrdinal;
    public long sourceLine;

    public PriceEvent() { }

    public static PriceEvent from(TradeEvent trade, int fileOrdinal) {
        PriceEvent event = new PriceEvent();
        event.symbol = trade.symbol();
        event.securityType = trade.securityType();
        event.price = trade.price();
        event.timestampMicros = trade.timestamp().toEpochSecond() * 1_000_000L
                + trade.timestamp().getNano() / 1_000;
        event.fileOrdinal = fileOrdinal;
        event.sourceLine = trade.sourceLine();
        return event;
    }

    public long timestampMillis() {
        return Math.floorDiv(timestampMicros, 1_000L);
    }
}

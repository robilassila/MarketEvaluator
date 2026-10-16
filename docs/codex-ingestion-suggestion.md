## Idea

**Use multiple readers, each tracking its own event-time progress, and let Flink coordinate when windows can close.** This lets processing start immediately while files are still being read.

The architecture becomes:

```text
Reader 1 → parse → timestamps + watermarks ─┐
Reader 2 → parse → timestamps + watermarks ─┼→ keyBy(symbol)
Reader 3 → parse → timestamps + watermarks ─┘       ↓
                                          Five-minute windows
                                                  ↓
                                          Per-symbol EMAs
```

Each reader maintains its own latest timestamp and emits:

```text
reader watermark = reader’s latest timestamp − delay − 1 ms
```

Downstream, Flink uses the **minimum watermark across active inputs**. This prevents a fast reader from closing windows while another reader is still behind. [Flink watermark documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/)

For example:

| Reader | Latest timestamp | Watermark with 65-second delay |
|---|---|---|
| A | 09:12:00 | 09:10:54.999 |
| B | 09:07:00 | 09:05:54.999 |

The combined watermark is **09:05:54.999**. The `[09:00, 09:05)` windows can close, but `[09:05, 09:10)` windows stay open. Those closed windows can immediately feed the EMA operator—even though neither reader has finished.

For our implementation, this requires three changes:

1. **Give readers separate input splits.** Replace the single `ReplaySplit` with multiple splits assigned to parallel readers. Splits must respect CSV record boundaries because quoted records can span lines. Preserve file ordinals and source lines.

2. **Generate watermarks at each reader.** Only eligible positive-price events advance time, matching the existing parsing rules.

3. **Replace the global lateness gate.** Its single latest-seen timestamp cannot safely represent multiple readers. Late-event handling must instead follow the coordinated window progress, with dropped records counted explicitly.

The closing-price selection and EMA formulas can stay.

**The input split strategy matters.** Giving one reader Monday and another Friday lets Friday run far ahead, accumulating records while Monday holds back window closure. Prefer splits covering similar time ranges, or coordinate their progress so fast readers cannot get too far ahead. Any reader must also account for earlier data in splits it has yet to read before announcing progress.

This design still permits late records: a reader’s watermark is a promise based on the configured disorder allowance, not proof that it has seen every earlier timestamp. It also changes the current global replay-based lateness semantics.

The result is **parallel reading and parsing, incremental window results, and bounded delay before EMA updates**, without waiting for the entire dataset.

# MarketEvaluator processing contract

This is the Part 1 contract for ingestion, five-minute closing prices, EMA38,
EMA100, and crossovers. The Java/Flink Query 1 processor implements ingestion, closing prices, and EMAs.
Crossover detection and subscription filtering remain for part 4.

## Requirements and chosen interpretations

The authority for the queries is `CS_E4780_Course_Project_2026.pdf`, Sections
2.2 and 3.1–3.3. The CSV header supplies field descriptions. The PDF requires
grouping by the complete symbol, computing both EMAs for all symbols, publishing
only subscribed symbols, five-minute tumbling windows aligned to dataset
midnight, and the stated crossover inequalities.

Zero-price exclusion is an explicit user requirement. The other entries below
are project defaults where the PDF requirements need interpretation:

| Question | Chosen interpretation |
| --- | --- |
| “Last observed” closing price | Greatest event timestamp, then greatest source position |
| Empty window | No result, no EMA update, no advisory |
| Overnight/day boundary | Keep each symbol's previous EMAs; do not reset or fill gaps |
| Initial EMA | Zero is the prior state; calculate the first populated window normally |
| CEST in November | Fixed UTC+02:00, as written in the PDF |
| Missing trading date | Use the system `Date`, without inferring a previous day |
| Zero price | Ignore before timestamp/window processing; count separately as `ignored_zero` |
| Duplicate timestamp/price/record | Preserve every eligible positive-price source record |
| Late event | Drop and count if its window was already finalized; no revisions |
| End of bounded input | Finalize all remaining populated windows |

These choices are exercised by the parser and Query 1 tests in
`market_evaluator/src/test/java/evaluator/`. Inspection documents referenced by
earlier versions of this contract are not present in this checkout.

## CSV and validation

Read UTF-8 CSV using a CSV parser, including quoted fields. Skip blank lines and
lines beginning with `#` outside a CSV record. The first remaining record is the
header. Strip surrounding whitespace from header names and field values; do not
split records using a plain comma string split. Date and time parsing is strict.
The observed files have no quoted or multiline records; the parser must still
respect CSV quoting. Retain physical starting line numbers for provenance.

The observed header has 39 columns. Accept extra trailing fields only if they
are empty after trimming. Reject short records and nonempty excess fields.
Map by header name; do not depend on column positions. A missing or repeated
required header is a file-level error, not a reason to skip every row.

Required header fields: `ID`, `SecType`, `Date`, `Time`, `Last`, `Trading date`,
and `Trading time`. Other columns are optional for the processor. In particular,
`Last trade time` (called “Last price time” in the PDF) is diagnostic and is not
the selected event clock. `Last volume` and `Total volume` distinguish some
equal-time trades but are not mandatory or used to deduplicate.

| Field | Meaning and use |
| --- | --- |
| `ID` | Full key, including `.FR`, `.NL`, or `.ETR`; never combine exchanges |
| `SecType` | `E` equity or `I` index; both participate |
| `Last` | Last trade price; a populated positive value is eligible for windows; zero is ignored |
| `Trading date` | Event date when supplied |
| `Trading time` | Time of last update; selected because it is marked relevant in the PDF |
| `Date`, `Time` | System update date/time; `Date` supplies the missing-date fallback |
| `Last trade time` | Compare with the chosen clock in inspection, but do not substitute |

Validate records in this order; assign exactly one rejection reason:

1. `shape`: short row or nonempty extra field.
2. `identity`: empty symbol, unknown exchange suffix, or security type other than `E`/`I`.
3. If `Last` is empty, count `non_price` and stop. It is not a rejected row.
4. `price`: `Last` cannot be parsed as a finite, nonnegative number.
5. If the parsed price is zero, count `ignored_zero` and stop. It is neither a
   rejected row nor a non-price row. Do not validate its event timestamp or create
   symbol/window state.
6. `event_timestamp`: selected date/time is missing or invalid.
7. `late`: the event's window has already been finalized.

Empty fields are the observed null representation. Do not interpret literal
`NULL`, `NaN`, or infinity as a missing value. Non-price rows do not update price
state or advance the event-time high-water mark. Zero-price records likewise
cannot advance time, replace a closing price, update EMAs, or trigger an advisory.
A window containing only zero-price/non-price records is empty under this contract.
System `Time` is diagnostic:
a missing value does not invalidate a price event with a usable event clock.
Likewise, invalid system `Date` matters only when needed as the event-date fallback.

Price text is parsed into a Java `double` without rounding to fewer decimal
places. All observed populated prices use six fractional digits. Repeated
positive prices, equal timestamps, and exact repeated positive-price records remain eligible.
Zero prices, including a parsed negative zero, are ignored by explicit user instruction.
There is no explicit unique trade identifier permitting reliable deduplication.

## Event time, source order, and finalization

Parse dates as `dd-MM-uuuu` and times as `HH:mm:ss` with an optional fractional
part of one to six digits. Retain microsecond precision for closing-price order;
observed files use milliseconds. The event date is `Trading date` if populated,
otherwise `Date`. A populated invalid trading date is rejected, not replaced.

Interpret the resulting local date/time at fixed offset `+02:00`. Do not apply
the host timezone or daylight-saving rules. The choice preserves the PDF's
literal CEST interpretation; actual Central European civil time in November
would use a different offset. Window labels always include the chosen offset.

Process daily files in ascending dataset date order, independent of CLI order.
Discover each daily file's date from its first strictly valid system `Date`,
without changing event timestamp validation. Reject undated files in multi-file
runs. For multiple files on the same date, sort by normalized path relative to
the invocation directory. A single file needs no date-discovery pass.
A source position is the pair `(zero-based file ordinal, physical starting line)`.
Ingestion and the global lateness gate run at parallelism 1; keyed window and
EMA processing may run in parallel. Keep source positions through all stages.

Windows are `[start, end)`, five minutes long, beginning at local midnight.
A price exactly at `09:05:00` belongs to `[09:05,09:10)`. The closing record is
the maximum tuple `(event timestamp, file ordinal, physical starting line)`
among accepted price events in that symbol's window. Retain only this aggregate,
not every price, in the future streaming implementation.

Let `H` be the greatest accepted price-event timestamp seen so far, truncated
to milliseconds, and `D` the nonnegative configured watermark delay in milliseconds.
After processing a record, finalize every populated window whose end is at or
before `H - D`, in increasing window order for each symbol. A new record is late
if its window end was at or before that cutoff **before** the record arrived.
Equality at the cutoff finalizes the window. A record can be behind `H` by more
than `D` and still be accepted if its window remains open.

For Flink's millisecond window timers, the equivalent watermark is
`H - D - 1 ms`; a window's last timestamp is `end - 1 ms`. Never revise finalized
windows or downstream EMA state. The singleton lateness gate applies this logical acceptance order before
watermark assignment. The implemented generator emits the equivalent watermark
after each forwarded positive-price event. The single ordered source avoids
progress coordination between concurrent source partitions.

The initial delay is **65 seconds (65,000 milliseconds)**: the maximum global
high-water disorder among accepted positive-price events across the inspected
daily inputs, rounded up to whole seconds. The retained Part 1 estimate is 64.517 seconds. The original inspection artifacts
are absent from this checkout, so that full-input estimate is not independently
reverified by the current sample tests. This is an empirical default for
these inputs, not a guarantee for unseen days. Exclude zero prices when measuring
it; they do not participate in event-time progress. Preserve missing-date
fallbacks for positive prices. Full input completion advances the watermark to
infinity and finalizes all remaining populated windows. No empty windows are
generated.

## Indicators and advisories

Each full symbol has independent `(previousEMA38, previousEMA100)` state,
initially `(0,0)`. For each finalized populated window:

```text
alpha_j = 2.0 / (1.0 + j)
EMA_j = close * alpha_j + previousEMA_j * (1.0 - alpha_j)
```

Use `j = 38` and `j = 100`, Java `double` arithmetic, and the operation order
shown above. Do not round intermediate values or use a fused multiply-add.
Compare the current pair with the prior state before replacing it:

```text
BUY  when current38 > current100 and previous38 <= previous100
SELL when current38 < current100 and previous38 >= previous100
```

Equality in the current pair emits no advisory. An initial positive close emits
BUY because both prior EMAs are zero. Zero prices never produce a closing price.
Empty intervals preserve both state values and produce no comparison.
Compute and detect for all symbols; subscriptions only filter published records.

Query 1 tests assert symbol, window boundaries, closing price, both EMAs, and
closing provenance directly; CLI tests also verify offset-qualified formatting.
Advisory tests belong to part 4. Expected-result JSON files are not retained. Indicator verification permits absolute or relative error
`1e-9`: `abs(actual - expected) <= max(1e-9, 1e-9 * abs(expected))`.
This tolerance never changes crossover inequalities or advisory expectations.

## Query 1 implementation

`evaluator.processing.QueryOnePipeline` builds the bounded streaming graph:
ordered CSV source → global lateness gate → timestamp/watermark assignment →
keyed incremental five-minute aggregation → keyed EMA state.

Only the closing candidate is retained per open symbol/window. Finalized windows
are emitted chronologically per symbol, with duplicate/decreasing window guards.
No empty windows are generated. EMA state persists across gaps and day boundaries.
Records use primitive epoch microseconds for closing order and epoch milliseconds
for Flink window timers; output is rendered at fixed `+02:00`.

The CLI consumes indicators incrementally and reports Flink accumulators after
successful completion. It owns the local MiniCluster lifecycle: completion does
not stop the cluster until result collection, counter retrieval, and iterator
cleanup are finished. Failure paths also close the cluster. Per-file parser summaries contribute counts without
creating event-time progress. Automatic restart and checkpoint recovery are
disabled. Results printed before a failure are partial, and replay prints results
again; stdout has no exactly-once publication guarantee.

No standalone sequential reference calculator is present in this checkout.
Tests use independent fixture arithmetic and a sample-specific independent scan.

## Query 1 validation

The included `data/sample-positive-prices-08.csv` contains 10,000 positive-price
records across 1,227 symbols. At the default 65-second delay, independent scanning
finds zero late events and 1,263 populated symbol/windows. The Flink test compares
all closing records, provenance, and EMA values against this independent scan
at processing parallelism 1, 2, and 4.

For `IEBBB.FR`, the independently selected closes are all 52.69:

| Window start (+02:00) | Closing source line | EMA38 | EMA100 |
| --- | ---: | ---: | ---: |
| 2021-11-08 07:00 | 242 | 2.702051282051282 | 1.0433663366336634 |
| 2021-11-08 07:05 | 491 | 5.265535831689677 | 2.0660719537300265 |
| 2021-11-08 07:10 | 722 | 7.697559635192770 | 3.0685259744482436 |
| 2021-11-08 07:15 | 9566 | 10.004864269285449 | 4.051129420498773 |

The first state update is `52.69 * 2/39` and `52.69 * 2/101`, from zero priors.
Subsequent updates apply the stated recurrence without rounding.

The sample oracle deliberately handles the sample's unquoted, positive-price
rows only, and asserts those assumptions. Parser behavior for quoted/multiline
records and invalid inputs is checked separately. Fixture tests provide explicit
expectations for late events, exact boundaries, timestamp ties, empty windows,
zero/non-price records, overnight gaps, file ordering, and bounded completion.
This validation does not establish performance or correctness on all unseen data.

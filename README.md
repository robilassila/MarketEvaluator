# MarketEvaluator

## Current application status

The bounded Apache Flink job incrementally reads CSV files, selects
five-minute closing prices, calculates EMA38 and EMA100 for all symbols, and
emits BUY/SELL advisories on EMA crossovers. Indicators and advisories are
printed to stdout by default. Optional static subscriptions filter both
published record types after all symbols have been calculated.

The semantics are specified in [the processing contract](docs/processing-contract.md).

## Run locally

Install JDK 17 and Maven. Flink dependencies are pinned to 2.3.0 and Maven
starts a local Flink runtime.

Ensure Maven uses JDK 17 with `mvn -version`. On macOS:

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
export PATH="$JAVA_HOME/bin:$PATH"
mvn -version
```

From the repository root:

```bash
cd market_evaluator
mvn compile exec:exec \
  -Devaluator.args="../data/sample-positive-prices-08.csv"
```

The application accepts one or more CSV file paths and these optional settings:

```text
evaluator.Main [--parallelism N] [--watermark-delay-ms D]
  [--subscribe FULL_SYMBOL ...] [--output-dir NEW_DIRECTORY] <csv-file>...
```

```bash
mvn compile exec:exec \
  -Devaluator.args="--parallelism 4 --watermark-delay-ms 65000 \
    --subscribe IEBBB.FR --subscribe CROSS.NL \
    --output-dir ../results/week-run-01 /path/to/day-09.csv /path/to/day-08.csv"
```

Parallelism defaults to 1 and watermark delay to 0 ms. With no `--subscribe`
options, every symbol is published; each repeated option selects one full
symbol identifier (including its exchange suffix), and the same allowlist
filters indicators and advisories. EMA state and crossover detection always
run for all symbols.

Without `--output-dir`, indicator CSV rows and `ADVISORY` CSV rows are printed
to stdout, followed by processing and published-record counts. With
`--output-dir`, choose a directory that does not already exist. After a
successful run, it contains `indicators.csv`, `advisories.csv`, and
`run-metadata.csv`. The CSVs include headers, event timestamps use the
contract's fixed UTC+02:00 offset, and metadata records the inputs, settings,
subscriptions, counters, and published-record counts. Output record order is
not guaranteed across parallel executions.

The output is staged in a temporary sibling directory and exposed at the
requested path only after successful processing and an atomic directory move.
An existing output path is rejected rather than overwritten. A failed run
does not publish a completed output directory; a process crash can leave a
hidden temporary staging directory. The bounded source and local output flow
do not provide a verified exactly-once guarantee.

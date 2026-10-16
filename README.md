# MarketEvaluator

## Current application status

Query 1 runs as a bounded Apache Flink streaming job: incrementally read CSV
files, select five-minute closing prices, and calculate EMA38 and EMA100 for
all symbols. Finalized indicators and a final processing summary are printed
to stdout. Query 2 advisories, subscriptions, and structured file sinks remain
for part 4.

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

The application accepts one or more CSV file paths, plus optional processing
parallelism (default 1) and watermark delay (default 0):

```text
evaluator.Main [--parallelism N] [--watermark-delay-ms D] <csv-file>...
```

```bash
mvn compile exec:exec \
  -Devaluator.args="--parallelism 4 --watermark-delay-ms 65000 /path/to/day-09.csv /path/to/day-08.csv"
```

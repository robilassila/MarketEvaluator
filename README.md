# MarketEvaluator

## Current application status

The bounded Apache Flink job incrementally reads CSV files, selects
five-minute closing prices, calculates EMA38 and EMA100 for all symbols, and
emits BUY/SELL advisories on EMA crossovers. Indicators and advisories are
written as CSV files. Optional static subscriptions filter both record types
after all symbols have been calculated.

The semantics are specified in [the processing contract](docs/processing-contract.md).

## Get the data

The repository includes only a 10,000-row sample,
`data/sample-positive-prices-08.csv`. The full input is the
[DEBS 2022 Grand Challenge trading data](https://doi.org/10.5281/zenodo.6382482):
one CSV per day from 8 to 14 November 2021, about 25 GB in total. From the
repository root:

```bash
scripts/download-data.sh
```

This downloads the day files into `data/`, which Git ignores. The script's
header comment describes verification, resuming and the optional target
directory.

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
  -Devaluator.args="--output-dir ../results/sample ../data/sample-positive-prices-08.csv"
```

The application accepts one or more CSV file paths:

```text
evaluator.Main [--parallelism N] [--subscribe FULL_SYMBOL ...]
  --output-dir DIRECTORY <csv-file>...
```

```bash
mvn compile exec:exec \
  -Devaluator.args="--parallelism 4 \
    --subscribe IEBBB.FR --subscribe CROSS.NL \
    --output-dir ../results/two-days \
    ../data/debs2022-gc-trading-day-09-11-21.csv \
    ../data/debs2022-gc-trading-day-08-11-21.csv"
```

Input files are processed in file-name order, which is chronological for the
DEBS file names. Parallelism defaults to 1. With no `--subscribe`
options, every symbol is published; each repeated option selects one full
symbol identifier (including its exchange suffix), and the same allowlist
filters indicators and advisories. EMA state and crossover detection always
run for all symbols.

The output directory is created if needed, and `indicators.csv` and
`advisories.csv` in it are overwritten. Both files have headers, and
timestamps use the contract's fixed UTC+02:00 offset. Record order is not
guaranteed when processing in parallel. A failed run leaves partial files.

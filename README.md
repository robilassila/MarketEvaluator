# MarketEvaluator

## Current application status

The application currently reads and validates CSV input, then prints each
accepted price event and a parsing summary. Window aggregation, EMA calculation,
crossover detection, and the downstream event consumer have not been implemented
yet; those will replace the temporary event-printing behavior as development
continues.

## Run locally

Install JDK 17 or newer and Maven.

From the repository root, compile and run the current parser-only application
on the included sample:

```bash
cd market_evaluator
mvn compile exec:java \
  -Dexec.mainClass=evaluator.Main \
  -Dexec.args="../data/sample-positive-prices-08.csv"
```

The CSV path is a required program argument. The parser prints a legend, then
one line for each accepted event, followed by a summary. The included sample
prints 10,000 event lines. To parse a different file, pass its path instead:

```bash
mvn compile exec:java \
  -Dexec.mainClass=evaluator.Main \
  -Dexec.args="/path/to/trades.csv"
```

If the path is missing or extra arguments are provided, the application prints
usage instructions. If the file cannot be read or parsed, it prints an error
and exits with a non-zero status.

## Unit testing

Run the project test suite with Maven:

```bash
cd market_evaluator
mvn test
```

This executes the Java tests for the parser and validation behavior, including
fixture-driven checks for:

- window ordering and source provenance
- zero-price handling before timestamp validation
- quoted and multiline CSV parsing
- invalid or missing required headers
- missing `Trading date` fallback to `Date`
- invalid populated `Trading date` rejection
- literal special price values like `NULL`, `NaN`, and `Infinity`

The tests are designed to match the parsing contract and dataset expectations.
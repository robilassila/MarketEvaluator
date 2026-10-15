# MarketEvaluator implementation plan

## Goal and current state

Implement the two queries in `CS_E4780_Course_Project_2026.pdf`: five-minute price windows with EMA indicators, followed by buy/sell advisories when those indicators cross. Evaluate correctness, scalability, and resource use. Add visualization after the required processing works.

The repository contains a Java 17 Maven project in `market_evaluator/`, Apache Flink 2.3.0, and a weekday sample dataset in `data/sample-positive-prices-08.csv`. Ingestion and Query 1 are implemented; Query 2 and subscription/file output remain next.

## Proposed architecture

- Use Java and Apache Flink for the initial implementation, following the existing Maven setup. Confirm this choice before expanding the application if Scala or C++ is still preferred.
- Replay CSV files incrementally in streaming mode; use dataset timestamps rather than wall-clock time for windows.
- Group events by the complete symbol identifier, including the exchange.
- Select the last trade price in each five-minute tumbling window.
- Maintain EMA38 and EMA100 state per symbol and detect crossovers between consecutive evaluated windows.
- Calculate indicators for all symbols; filter published updates by the configured subscription list.
- Write structured result files initially. A separate database and Kafka are not required for this scope.
- Start locally, then evaluate parallel workers on one machine. Distributed deployment is an extension if the course rubric or our experiments warrant it.

## 1. Inspect the data and settle processing rules

- Inspect the CSV header and representative rows: delimiter, quoting, null markers, price format, date/time precision, and event types.
- Use the existing weekday sample in `data/sample-positive-prices-08.csv` as the current validation dataset; it already contains active trading-day data suitable for correctness checks.
- Map the relevant fields: symbol, security type, last trade price, trading date/time, and system date/time.
- Determine which fields identify a new price event and which timestamp should assign it to a window. Do not treat a repeated or carried price field as a new trade without checking the data semantics.
- Measure timestamp disorder and duplicate timestamps on a sample. Define whether "last observed" means arrival order or greatest event timestamp, and specify a deterministic tie-breaker.
- Resolve the PDF's timezone terminology: it calls November timestamps CEST. Document the interpretation used for midnight alignment.
- Define behavior for empty windows, overnight gaps, day boundaries, malformed rows, and the final window at end of input. Clarify ambiguous requirements with the teacher if needed.
- Define EMA initialization precisely. The initial prior EMA is zero; the wording around the first window needs an explicit interpretation. A first positive update can satisfy the stated buy condition.
- Document a late-event policy and a watermark strategy. Revisions to earlier windows would also affect subsequent EMAs, so choose a policy deliberately and count late events.

Completed: See [processing contract](docs/processing-contract.md)

## 2. Prepare the Maven project

- Select and pin a stable Flink version compatible with the chosen JDK; align dependency and runtime versions.
- Add the necessary streaming, local runtime, CSV parsing, logging, and test dependencies.
- Configure the Maven execution plugin and retain a simple local run command in `README.md`.
- Introduce configuration for input paths, subscriptions, parallelism, output directory, and watermark delay.
- Support a normal full-speed processing mode; add rate-controlled replay for demonstrations and load experiments if needed.
- Organize code into ingestion, models, processing, output, and configuration packages. Keep the entry point small.
- Keep full datasets and generated outputs outside Git. Commit only small fixtures that are suitable for redistribution, with attribution where applicable.

Completed prerequisite setup: pinned Flink 2.3.0, runtime logging, JUnit, Maven execution, and configurable input paths/processing parallelism/watermark delay. Part 3 supplies the terminating local Flink job. Output-directory configuration and subscriptions remain with part 4; rate-controlled replay remains optional.

## 3. Implement ingestion and Query 1

- Read files without loading the dataset into memory. Process daily files in chronological order and preserve the agreed source-order semantics.
- Parse only the required fields while validating records and counting rejected rows.
- Follow the processing contract: non-price and zero-price records cannot advance time or contribute to a closing price.
- Assign timestamps and generate watermarks before grouping by symbol. Handle idle input partitions if relevant.
- Implement five-minute windows aligned to dataset midnight, with explicit boundary behavior.
- Aggregate closing prices incrementally rather than storing every price event in a window.
- Update per-symbol EMA state only when a window is finalized, in chronological window order. Implement the agreed empty-window behavior explicitly.
- Emit indicator records containing symbol, window boundaries, closing price, EMA38, and EMA100.

Completed: bounded ordered ingestion, deterministic lateness decisions, incremental five-minute windows, and keyed EMA state. End-to-end Flink fixture tests match independent expectations, including out-of-order/late events and final-window flushing. The complete 10,000-event sample matches an independent scan at processing parallelism 1, 2, and 4: 1,263 indicators, 1,227 symbols, and zero late events. See [Query 1 validation](docs/processing-contract.md#query-1-validation).

## 4. Implement Query 2, subscriptions, and output

- Compare current and previous EMA values using the exact crossover inequalities.
- Emit one advisory per qualifying finalized window, with a deterministic identity based on symbol, window, and direction.
- Apply subscription filtering after calculating indicators and detecting patterns for all symbols.
- Begin with a configured subscription list. Add live subscription changes only if needed for the demonstration.
- Write indicator and advisory records to structured files, including run metadata so outputs from different experiments remain distinguishable.
- Document output behavior on replay or failure; do not claim exactly-once results without verifying both source and sink guarantees.

Completion: both queries match independently calculated fixture expectations, including BUY/SELL and no-signal cases, and changing subscriptions changes published results without changing calculations.

## 5. Evaluate correctness and performance

- Derive expected results independently from the processing contract for small CSV fixtures. Calculate closing records, EMA38/EMA100, and advisories by hand or with separate arithmetic; do not generate expectations using the production calculation code.
- Cover exact five-minute boundaries, equal timestamps and source-position tie-breaks, out-of-order and late events, empty windows, overnight gaps, zero prices, EMA initialization, crossover equality, and final-window flushing at bounded input completion.
- Test parsing, closing-price selection, EMA updates, and crossover conditions individually, then run the actual CSV-to-output Flink pipeline on the fixtures with every operator configured at parallelism 1. Assert complete output records, including closing provenance and exact advisory expectations; use the processing contract's absolute or relative tolerance of 1e-9 for EMA values.
- Independently inspect selected windows and calculate short EMA sequences from the real-data sample. Check invariants on larger inputs: each close belongs to an eligible event in its window, closing selection follows the contract, symbols have independent state, and subscription changes do not alter calculations.
- Run the same input at parallelism 1, 2, and 4 where hardware permits, preserving the specified source-order and late-event semantics. Compare complete results by symbol and window, ignoring output order while checking for missing or duplicate records. Require identical closing records and advisories, with the documented tolerance for EMAs.
- Treat agreement across parallelism settings as evidence of parallel execution consistency. Independently calculated expectations establish correctness for tested cases; a parallelism-1 run alone does not establish correctness. Report test coverage and limitations explicitly.
- Benchmark increasing input sizes and available worker counts, for example 1, 2, 4, and 8 where hardware permits.
- Measure input rows per second, relevant price events per second, total runtime, CPU, memory, output volume, and backpressure.
- Separate processing latency from watermark waiting time and from the historical timestamps in the dataset.
- Investigate heavily traded symbols, source throughput, and sink throughput as potential scaling bottlenecks.
- Record hardware, software versions, settings, warm-up procedure, repeated measurements, and variability. Label single-machine scaling accurately.
- Attempt full-week processing after smaller runs are correct and resource requirements are understood.

Completion: passing unit and end-to-end fixture tests, documented independent sample checks and invariants, equivalent outputs across tested parallelism settings, and reproducible benchmark commands, saved measurements, and charts supporting the report's conclusions.

## 6. Add visualization for bonus points

- Choose a lightweight plotting or dashboard tool after the output format is stable.
- Show a selected symbol's five-minute closing prices, both EMA curves, and BUY/SELL markers.
- Include exchange, time range, and advisory details so signals can be traced to their source windows.
- Demonstrate subscription selection and replay progression if practical.

Completion: a clear demonstration using real weekday data, with plotted values verified against processor output.

## 7. Prepare submission

- Write the 4–5 page report using the ACM template: abstract, introduction, architecture, implementation, evaluation, conclusion, and references.
- Explain semantic assumptions, design tradeoffs, measured scaling limits, and any unimplemented extensions.
- Update the README with setup, data acquisition, execution, testing, and benchmark instructions.
- Verify the system from a clean checkout and ensure the submission repository is public.
- State each member's contribution if this is a group project.
- Submit the repository link and report by 27 November 2026.

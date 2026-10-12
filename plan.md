# MarketEvaluator implementation plan

## Goal and current state

Implement the two queries in `CS_E4780_Course_Project_2026.pdf`: five-minute price windows with EMA indicators, followed by buy/sell advisories when those indicators cross. Evaluate correctness, scalability, and resource use. Add visualization after the required processing works.

The repository currently contains a Java 17 Maven project in `market_evaluator/`, a placeholder `evaluator.Main`, and a weekday sample dataset in `data/sample-positive-prices-08.csv`. Flink has not yet been added.

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

Completion: a documented processing contract and small representative fixtures with expected outcomes.

## 2. Prepare the Maven project

- Select and pin a stable Flink version compatible with the chosen JDK; align dependency and runtime versions.
- Add the necessary streaming, local runtime, CSV parsing, logging, and test dependencies.
- Configure the Maven execution plugin and retain a simple local run command in `README.md`.
- Introduce configuration for input paths, subscriptions, parallelism, output directory, and watermark delay.
- Support a normal full-speed processing mode; add rate-controlled replay for demonstrations and load experiments if needed.
- Organize code into ingestion, models, processing, output, and configuration packages. Keep the entry point small.
- Keep full datasets and generated outputs outside Git. Commit only small fixtures that are suitable for redistribution, with attribution where applicable.

Completion: a reproducible build and a minimal local Flink job that reads a fixture and terminates successfully.

## 3. Build a sequential correctness reference

- Implement a simple calculator independent of Flink's window and state machinery, using the agreed processing contract.
- Produce expected closing prices, EMA values, and advisories for small inputs.
- Use the formula `EMA_j = close * (2 / (1 + j)) + previous_EMA_j * (1 - 2 / (1 + j))` for `j = 38` and `j = 100`.
- Emit BUY when the current EMA38 is greater than EMA100 and the previous EMA38 was less than or equal to EMA100.
- Emit SELL when the current EMA38 is less than EMA100 and the previous EMA38 was greater than or equal to EMA100.
- Test exact window boundaries, multiple exchanges, equality, both crossover directions, initialization, empty windows, and ordering rules. Choose and document numeric precision and comparison behavior.

Completion: hand-checked fixtures and a reference output for a representative real-data sample.

## 4. Implement ingestion and Query 1

- Read files without loading the dataset into memory. Process daily files in chronological order and preserve the agreed source-order semantics.
- Parse only the required fields while validating records and counting rejected rows.
- Ensure non-price records can advance time where appropriate, even if they do not contribute to the window closing price.
- Assign timestamps and generate watermarks before grouping by symbol. Handle idle input partitions if relevant.
- Implement five-minute windows aligned to dataset midnight, with explicit boundary behavior.
- Aggregate closing prices incrementally rather than storing every price event in a window.
- Update per-symbol EMA state only when a window is finalized, in chronological window order. Implement the agreed empty-window behavior explicitly.
- Emit indicator records containing symbol, window boundaries, closing price, EMA38, and EMA100.

Completion: Query 1 matches reference results on fixtures and a real-data sample, including out-of-order cases.

## 5. Implement Query 2, subscriptions, and output

- Compare current and previous EMA values using the exact crossover inequalities.
- Emit one advisory per qualifying finalized window, with a deterministic identity based on symbol, window, and direction.
- Apply subscription filtering after calculating indicators and detecting patterns for all symbols.
- Begin with a configured subscription list. Add live subscription changes only if needed for the demonstration.
- Write indicator and advisory records to structured files, including run metadata so outputs from different experiments remain distinguishable.
- Document output behavior on replay or failure; do not claim exactly-once results without verifying both source and sink guarantees.

Completion: both queries match the reference, and changing subscriptions changes published results without changing calculations.

## 6. Evaluate correctness and performance

- Compare complete outputs against the reference on small samples and a larger weekday input, with a documented floating-point tolerance for indicator values.
- Test the same input at different parallelism settings and confirm equivalent results.
- Benchmark increasing input sizes and available worker counts, for example 1, 2, 4, and 8 where hardware permits.
- Measure input rows per second, relevant price events per second, total runtime, CPU, memory, output volume, and backpressure.
- Separate processing latency from watermark waiting time and from the historical timestamps in the dataset.
- Investigate heavily traded symbols, source throughput, and sink throughput as potential scaling bottlenecks.
- Record hardware, software versions, settings, warm-up procedure, repeated measurements, and variability. Label single-machine scaling accurately.
- Attempt full-week processing after smaller runs are correct and resource requirements are understood.

Completion: reproducible benchmark commands, saved measurements, and charts supporting the report's conclusions.

## 7. Add visualization for bonus points

- Choose a lightweight plotting or dashboard tool after the output format is stable.
- Show a selected symbol's five-minute closing prices, both EMA curves, and BUY/SELL markers.
- Include exchange, time range, and advisory details so signals can be traced to their source windows.
- Demonstrate subscription selection and replay progression if practical.

Completion: a clear demonstration using real weekday data, with plotted values verified against processor output.

## 8. Prepare submission

- Write the 4–5 page report using the ACM template: abstract, introduction, architecture, implementation, evaluation, conclusion, and references.
- Explain semantic assumptions, design tradeoffs, measured scaling limits, and any unimplemented extensions.
- Update the README with setup, data acquisition, execution, testing, and benchmark instructions.
- Verify the system from a clean checkout and ensure the submission repository is public.
- State each member's contribution if this is a group project.
- Submit the repository link and report by 27 November 2026.

## Immediate next step

Inspect the available CSV and a weekday sample, then write down timestamp, closing-price, empty-window, and late-event rules. These decisions should precede the Flink pipeline because they determine both implementation and expected results.

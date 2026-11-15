# MarketEvaluator

## Current application status

A replay producer streams the trading CSVs into Kafka. An Apache Flink job
reads them in parallel, selects five-minute closing prices, calculates EMA38
and EMA100 for all symbols, and emits BUY/SELL advisories on EMA crossovers to
Kafka topics. Subscribers ("traders") read those topics, keep their
subscribed symbols and write them as CSV files.

The query is specified in the course project description,
[CS_E4780_Course_Project_2026.pdf](CS_E4780_Course_Project_2026.pdf).

```text
CSV files ─► TickProducer ─► topic "ticks" ─► Flink job (Main) ─► topics "indicators", "advisories" ─► Subscriber ─► CSV files
             (drops late     (16 partitions,   (parallelism P ≤                                          (filters by
              events)         keyed by symbol)  partition count)                                          --subscribe)
```

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

Everything runs in Docker

`docker-compose.yml` defines three services:

- `kafka`, a single-node broker. Containers reach it at `kafka:19092`, the host
  at `localhost:9092`.
- `evaluator`, the image built from `market_evaluator/Dockerfile`. Its first
  argument selects the program: `producer`, `job` or `subscriber`. It mounts
  `data/` (read-only) and `results/`, so paths are relative to the repository
  root. `market_evaluator/docker-entrypoint.sh` lists its environment variables.
- `test`, which runs Maven over the mounted sources.

`docker compose run` builds the image on first use and starts Kafka first.
After changing Java code, rebuild it with `docker compose build evaluator`.
Run a program with invalid arguments to see its full usage.

```bash
# 1. Load the ticks topic. Files are replayed in file-name order, which is chronological.
docker compose run --rm evaluator producer data/sample-positive-prices-08.csv

# 2. Run the job over everything in the topic, then stop.
docker compose run --rm evaluator job --bounded --parallelism 4

# 3. Write the results for two symbols to CSV files.
docker compose run --rm evaluator subscriber \
  --subscribe IEBBB.FR --subscribe CROSS.NL --output-dir results/sample
```

Topics live in the Kafka container, so `docker compose down` deletes them;
`docker compose stop` keeps them.

Things worth knowing:

- **The producer drops late events.** These are events whose five-minute
  window has already ended at the highest event time replayed so far
  (5,858 of the 59.4 million events in the full data set). Dropping them in the
  single-threaded producer, and not in the parallel job, keeps the output the
  same for every parallelism and partition count. `LateEventFilter` explains
  why.
- **Topics must start empty.** The job reads the ticks topic from the
  beginning, so loading the same files twice duplicates every event.
  Delete the topic first:
  `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --delete --topic ticks`.
  The same applies to the output topics between job runs. Alternatively,
  give each run its own topics with the `--*-topic` options.
- **Parallelism can't exceed the partition count.** The producer creates the
  ticks topic with 16 partitions by default. The job refuses a parallelism
  above the topic's partition count.
- **`--bounded` stops at the end of the topic.** Without it, the job runs
  live until killed. In live mode, the last windows never fire, because no
  later event advances event time past them.
- **The subscriber's stop condition depends on `--idle-timeout`.** By default
  it stops at the end of the output topics as they were when it started. With
  `--idle-timeout`, it follows the topics until they go quiet for that long.
- **Output format.** Output lines have the columns given by the CSV headers.
  Timestamps use a fixed UTC+02:00 offset. Record order isn't guaranteed.

## Benchmarks

Both scripts build the image, start the broker and run every program in a
container. Their header comments list the options.

- `scripts/benchmark-throughput.sh` measures resource scaling. It runs the
  bounded job over the loaded ticks topic at P = 1, 2, 4, 8 and 16, and writes
  events/s to `results/throughput.csv`.
- `scripts/benchmark-latency.sh <csv-file>...` measures workload scaling. For
  each producer rate, it starts the job live on a fresh topic, replays the
  files at that rate, and records latency percentiles in `results/latency/`.

Latency of a window runs from the moment the producer sends the first event
at or after the window's end, to the moment the job writes the window's
indicator to Kafka. The first moment comes from the producer's
`--boundary-log`; the second is the output record's Kafka timestamp. A window
fires only once every partition has passed its end. At low rates, sparsely
populated partitions take longer to get there, so latency can *fall* as the
rate rises until the system saturates.

Kafka, Flink and the producer share the same machine and CPUs, which limits
how far the results generalise. On macOS and Windows they also share the
Docker Desktop VM, whose CPU and memory limits (Settings → Resources) cap the
throughput at high parallelism.

## Tests

```bash
docker compose run --rm test        # mvn verify: unit tests and *IT tests
docker compose run --rm test test   # unit tests only
```

The `*IT` tests start their own Kafka container through Testcontainers. The
`test` service mounts the host's Docker socket for this; the comments in
`docker-compose.yml` explain how the tests reach that container.

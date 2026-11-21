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
- `visualizer`, a small browser dashboard backed by the Kafka indicator and
  advisory topics. It is available on `http://localhost:8080` and keeps the
  most recent 240 indicators and 100 advisories per symbol in memory.
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

## Live visualizer

Start Kafka and load some ticks, then keep the evaluator job and visualizer
running in separate terminals:

```bash
docker compose up -d kafka
docker compose run --rm evaluator producer data/sample-positive-prices-08.csv
docker compose run --rm evaluator job --parallelism 4
# In another terminal:
docker compose run --rm --service-ports visualizer
```

Open `http://localhost:8080`, enter a full symbol such as `IEBBB.FR`, and
subscribe. The chart plots the five-minute close, EMA38 and EMA100; crossover
advisories appear alongside it. The visualizer reads both output topics from
their beginning when it starts, then follows new records. Keep the job running
for a live feed. The visualizer is read-only and does not change topic data.

### Crossover demo

`data/ema-crossover-demo.csv` is a small synthetic feed for `DEMO.FR`. Its
price alternates between sustained high and low levels to produce repeated
EMA38/EMA100 crossovers. It contains 841 five-minute points and is paced at one
event per second below, so the replay lasts about 14 minutes. Use one Kafka
partition because the file contains only one symbol.

```bash
docker compose up -d kafka
docker compose build evaluator
```

In separate terminals, start the producer, job and visualizer. Start the job
shortly after the producer creates its input topic:

```bash
docker compose run --rm evaluator producer \
  --topic demo-ticks --partitions 1 --rate 1 data/ema-crossover-demo.csv
```

```bash
docker compose run --rm evaluator job --parallelism 1 \
  --ticks-topic demo-ticks --indicators-topic demo-indicators \
  --advisories-topic demo-advisories
```

```bash
docker compose run --rm --service-ports visualizer visualizer \
  --indicators-topic demo-indicators --advisories-topic demo-advisories
```

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

Both scripts run on the host. `scripts/benchmark.py` needs Python 3.10 or
later and nothing else. It builds the evaluator image, starts Kafka, runs every
program in a container and samples each container's CPU and memory with
`docker stats`. `scripts/plot-benchmarks.py` also needs matplotlib, which
`uv run` installs from the script's header. Without uv, install it with
`python3 -m pip install matplotlib` and run the script with `python3`.

```bash
# Resource scaling. Load the ticks topic first (see "Run locally").
python3 scripts/benchmark.py throughput [--parallelisms 1 2 4 8 16] [--repeat 3]

# Workload scaling. Creates and deletes its own topics; input files must lie under data/.
python3 scripts/benchmark.py latency [--rates 50000 100000 200000 400000] \
  [--parallelism 4] [--partitions 16] [--drain-timeout 10] data/<day-file>.csv...

# Figures for the report. The script's docstring lists them.
uv run scripts/plot-benchmarks.py
```

The brackets show each option's default.

`throughput` runs the bounded job once per parallelism and repetition. Each
run gets fresh output topics, which are deleted afterwards. The repetitions
give the plots their error bars.

`latency` handles one target rate at a time. It starts the job live on a fresh
ticks topic and waits until the job reports that it is running. Then it replays
the files at the target rate and runs the subscriber until the job's output
has been quiet for `--drain-timeout` seconds.

Each run overwrites the earlier results of the same benchmark:

| File | Contents |
|---|---|
| `results/throughput/throughput.csv` | `parallelism, repetition, events, seconds, events_per_second` |
| `results/throughput/resources.csv` | `parallelism, repetition, container, elapsed_s, cpu_percent, memory_bytes`, sampled over the whole job run |
| `results/latency/summary.csv` | `target_rate, achieved_rate, events, windows, p50_ms, p95_ms, p99_ms, max_ms` |
| `results/latency/resources.csv` | `target_rate, container, elapsed_s, cpu_percent, memory_bytes`, sampled during the replay |
| `results/latency/rate-<R>/` | `latency.csv` (one row per window), `boundaries.csv`, `producer.log`, `job.log` and the subscriber's output |
| `results/*/host.json` | CPUs and memory available to Docker |

About these files:

- **Resource samples** arrive about once per second per container. The
  `container` column is `kafka`, `job` or `producer`.
- **`cpu_percent`** is relative to one CPU, so it can exceed 100.
- **`achieved_rate`** can fall short of `target_rate` when the producer
  can't keep up.
- **Percentiles** use the nearest-rank method.

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

The benchmark scripts' parsers have their own tests:
`python3 -m unittest discover -s scripts`.

# MarketEvaluator

A streaming pipeline for the [DEBS 2022 Grand Challenge](https://doi.org/10.5281/zenodo.6382482)
trading data, built for the course project in
[CS_E4780_Course_Project_2026.pdf](CS_E4780_Course_Project_2026.pdf).
An Apache Flink job computes five-minute EMA38 and EMA100 per symbol and emits
BUY/SELL advisories on crossovers.

```text
CSV files ─► TickProducer ─► Kafka "ticks" ─► Flink job ─► Kafka "indicators", "advisories" ─► Subscriber / Visualizer
```

## Requirements

Docker with Compose. Python 3.10+ only for the benchmark scripts.

## Data

`data/` ships a 10,000-row sample and a synthetic crossover demo. For the full
data set (~25 GB):

```bash
scripts/download-data.sh
```

## Run

```bash
# 1. Replay CSV files into Kafka
docker compose run --rm evaluator producer data/sample-positive-prices-08.csv

# 2. Process everything in the topic, then stop (omit --bounded to run live)
docker compose run --rm evaluator job --bounded --parallelism 4

# 3. Write results for chosen symbols to CSV
docker compose run --rm evaluator subscriber \
  --subscribe IEBBB.FR --subscribe CROSS.NL --output-dir results/sample
```

Pass an invalid argument to any program to see its usage.

The job reads topics from the beginning, so topics must start empty. Reset with
`docker compose down`, or give each run its own topics via the `--*-topic` options.

## Visualizer

With the job running live (no `--bounded`):

```bash
docker compose run --rm --service-ports visualizer
```

Open <http://localhost:8080> and subscribe to a symbol such as `IEBBB.FR`.

For a feed with frequent crossovers, replay the demo file into its own topics,
each command in a separate terminal:

```bash
docker compose run --rm evaluator producer --topic demo-ticks --partitions 1 --rate 1 data/ema-crossover-demo.csv
docker compose run --rm evaluator job --parallelism 1 --ticks-topic demo-ticks \
  --indicators-topic demo-indicators --advisories-topic demo-advisories
docker compose run --rm --service-ports visualizer visualizer \
  --indicators-topic demo-indicators --advisories-topic demo-advisories
```

## Benchmarks

```bash
python3 scripts/benchmark.py throughput   # after loading the ticks topic
python3 scripts/benchmark.py latency data/<day-file>.csv
python3 scripts/plot-benchmarks.py
```

Results go to `results/`. Pass `--help` for options.

## Tests

```bash
docker compose run --rm test                 # unit and integration tests
python3 -m unittest discover -s scripts      # benchmark script tests
```

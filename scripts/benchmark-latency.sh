#!/usr/bin/env bash
# Workload scaling: for each rate, starts the job live (unbounded) on a fresh ticks topic, replays
# the given CSV files at that rate, then reads the results and reports latency percentiles.
# Latency is defined and computed by evaluator.consumer.Subscriber from the producer's boundary log.
#
# Usage: scripts/benchmark-latency.sh <csv-file>...   (files must lie under data/)
# Environment: RATES events/s (default "50000 100000 200000 400000"), PARALLELISM (default 4),
#              PARTITIONS (default 16)
# Per-rate output lands in results/latency/rate-<RATE>/; the summary in results/latency/summary.txt.
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <csv-file>..." >&2
  exit 2
fi
data="$(cd "$(dirname "$0")/../data" && pwd)"
inputs=()
for file in "$@"; do
  absolute="$(cd "$(dirname "$file")" && pwd)/$(basename "$file")"
  if [[ "$absolute" != "$data/"* ]]; then
    echo "$file is not under $data, which is the only input directory mounted into the container" >&2
    exit 2
  fi
  inputs+=("data/${absolute#"$data/"}")
done

source "$(dirname "$0")/benchmark-env.sh"

PARALLELISM="${PARALLELISM:-4}"
PARTITIONS="${PARTITIONS:-16}"
RESULTS="results/latency"
mkdir -p "$RESULTS"
: > "$RESULTS/summary.txt"

job=""
trap '[[ -n "$job" ]] && docker rm --force "$job" >/dev/null 2>&1 || true' EXIT

for rate in ${RATES:-50000 100000 200000 400000}; do
  run="latency-r$rate-$(date +%s)"
  directory="$RESULTS/rate-$rate"
  mkdir -p "$directory"
  # The job refuses to start without its input topic, and must be running before the replay begins.
  kafka_topics --create --topic "$run-ticks" --partitions "$PARTITIONS" --config retention.ms=-1 >/dev/null
  job="$run-job"
  docker compose run --detach --name "$job" -e LOG_LEVEL=error evaluator job --parallelism "$PARALLELISM" \
    --ticks-topic "$run-ticks" --indicators-topic "$run-indicators" --advisories-topic "$run-advisories" \
    >/dev/null
  sleep 15
  run_main producer --topic "$run-ticks" --partitions "$PARTITIONS" --rate "$rate" \
    --boundary-log "$directory/boundaries.csv" "${inputs[@]}" \
    | tee "$directory/producer.log"
  # Let the job drain what is already in Kafka before taking the end offsets.
  sleep 10
  summary="$(run_main subscriber --indicators-topic "$run-indicators" --advisories-topic "$run-advisories" \
    --boundary-log "$directory/boundaries.csv" --output-dir "$directory")"
  echo "$summary"
  echo "rate $rate: $(grep '^Latency' <<< "$summary")" >> "$RESULTS/summary.txt"
  docker stop "$job" >/dev/null
  docker logs "$job" > "$directory/job.log" 2>&1
  docker rm "$job" >/dev/null
  job=""
  for topic in ticks indicators advisories; do kafka_topics --delete --topic "$run-$topic"; done
done
cat "$RESULTS/summary.txt"

#!/usr/bin/env bash
# Resource scaling: runs the bounded job over the already-loaded ticks topic once per parallelism
# and records events/s. Load the topic first with the producer (see README).
# Each run writes to fresh output topics, which are deleted afterwards.
#
# Usage: scripts/benchmark-throughput.sh [output.csv]   (default: results/throughput.csv)
# Environment: PARALLELISMS (default "1 2 4 8 16")
set -euo pipefail
# Resolved before benchmark-env.sh changes to the repository root.
OUTPUT="${1:-}"
[[ -z "$OUTPUT" || "$OUTPUT" == /* ]] || OUTPUT="$PWD/$OUTPUT"
source "$(dirname "$0")/benchmark-env.sh"
OUTPUT="${OUTPUT:-$REPO/results/throughput.csv}"
mkdir -p "$(dirname "$OUTPUT")"
echo "parallelism,events,seconds,events_per_second" > "$OUTPUT"

for parallelism in ${PARALLELISMS:-1 2 4 8 16}; do
  run="bench-p$parallelism-$(date +%s)"
  report="$(run_main job --bounded --parallelism "$parallelism" \
    --indicators-topic "$run-indicators" --advisories-topic "$run-advisories")"
  echo "$report"
  # "Processed N events in S s (R events/s) at parallelism P"
  read -r events seconds rate < <(sed -E 's/^Processed ([0-9]+) events in ([0-9.]+) s \(([0-9]+) events\/s\).*/\1 \2 \3/' <<< "$report")
  echo "$parallelism,$events,$seconds,$rate" >> "$OUTPUT"
  kafka_topics --delete --topic "$run-indicators"
  kafka_topics --delete --topic "$run-advisories"
done
echo "Wrote $OUTPUT"

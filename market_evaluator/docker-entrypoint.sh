#!/bin/sh
# Usage: evaluator producer|job|subscriber|visualizer [options]
# Environment: BOOTSTRAP (default kafka:19092), LOG_LEVEL (slf4j-simple level, default info)
set -eu
case "${1:-}" in
  producer) class=evaluator.replay.TickProducer ;;
  job) class=evaluator.Main ;;
  subscriber) class=evaluator.consumer.Subscriber ;;
  visualizer) class=evaluator.visualizer.VisualizerServer ;;
  *) echo "Usage: evaluator producer|job|subscriber|visualizer [options]" >&2; exit 2 ;;
esac
shift
# The programs keep the last --bootstrap-servers they parse, so one given in "$@" overrides this default.
exec java -Dorg.slf4j.simpleLogger.defaultLogLevel="${LOG_LEVEL:-info}" -cp '/app/classes:/app/lib/*' \
  "$class" --bootstrap-servers "${BOOTSTRAP:-kafka:19092}" "$@"

# Shared setup for the benchmark scripts; source it, don't run it.
# Builds the evaluator image, starts Kafka, moves to the repository root and defines helpers that run
# the programs and Kafka's topic tool in containers. Paths given to the programs must be relative to
# the repository root and lie under data/ or results/, the only directories mounted into the container.

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

docker compose build evaluator
docker compose up --detach --wait kafka

run_main() {
  docker compose run --rm -T -e LOG_LEVEL=error evaluator "$@"
}

kafka_topics() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 "$@"
}

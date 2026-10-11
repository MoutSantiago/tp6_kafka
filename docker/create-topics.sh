#!/bin/bash
set -e

KAFKA_BROKER="kafka:9092"

topics=(
  "activation.requested"
  "billing.events"
  "provisioning.events"
  "activation.events"
  "activation.requested.dlq"
  "billing.events.dlq"
  "provisioning.events.dlq"
  "activation.events.dlq"
)

for topic in "${topics[@]}"; do
  echo "Creating topic: $topic"
  /opt/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server "$KAFKA_BROKER" \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --if-not-exists
done

echo "All topics created successfully"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "$KAFKA_BROKER"
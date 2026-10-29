#!/usr/bin/env bash
set -euo pipefail
for topic in _bank_v2_configs _bank_v2_offsets _bank_v2_status; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 \
    --config cleanup.policy=compact
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --entity-type topics --entity-name "$topic" --alter --add-config cleanup.policy=compact
done
for entity in customers cards transactions pipeline_barriers; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --if-not-exists --topic "$CDC_TOPIC_PREFIX.$BANK_SCHEMA.$entity" \
    --partitions 1 --replication-factor 1 --config message.timestamp.type=LogAppendTime
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --entity-type topics --entity-name "$CDC_TOPIC_PREFIX.$BANK_SCHEMA.$entity" \
    --alter --add-config message.timestamp.type=LogAppendTime
done

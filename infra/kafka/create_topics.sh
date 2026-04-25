#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Create all Kafka topics required by the MAS
# Run: bash infra/kafka/create_topics.sh
# Requires: Kafka running on localhost:9092
# ─────────────────────────────────────────────────────────────

BOOTSTRAP="localhost:9092"
REPLICATION=1
PARTITIONS=3

declare -A TOPICS_RETENTION=(
    ["pipeline.signals.raw"]="86400000"             # 24h
    ["agents.anomalies"]="604800000"                # 7 days
    ["agents.confirmed_incidents"]="2592000000"     # 30 days
    ["agents.actions_taken"]="7776000000"           # 90 days
    ["agents.heartbeats"]="3600000"                 # 1h
)

echo "Creating MAS Kafka topics on $BOOTSTRAP..."

for TOPIC in "${!TOPICS_RETENTION[@]}"; do
    RETENTION="${TOPICS_RETENTION[$TOPIC]}"
    echo "→ Creating topic: $TOPIC (retention: ${RETENTION}ms)"
    kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --if-not-exists \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" \
        --config "retention.ms=$RETENTION"
done

echo ""
echo "✅ Topics created:"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | grep -E "pipeline\.|agents\."

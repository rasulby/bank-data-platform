#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

set -a
source .env
set +a

echo "Waiting for Kafka Connect..."
until curl -sf http://localhost:8083/ > /dev/null; do
  sleep 3
done

# Replace template placeholders with environment variables
python3 -c 'import os, sys; sys.stdout.write(os.path.expandvars(open("infra/debezium/connector.template.json").read()))' \
  | curl -sS --fail-with-body -X PUT \
      -H "Content-Type: application/json" \
      --data @- \
      http://localhost:8083/connectors/bank-source-connector/config
echo
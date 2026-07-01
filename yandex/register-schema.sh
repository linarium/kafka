#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA=$(tr -d '\n' < "$ROOT/schemas/order-event.avsc" | python3 -c 'import json,sys; print(json.dumps({"schema": sys.stdin.read()}))')

curl -sS -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$SCHEMA" \
  http://localhost:8081/subjects/orders-events-value/versions

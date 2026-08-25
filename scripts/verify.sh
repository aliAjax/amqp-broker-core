#!/bin/sh
set -eu
base_url="${BROKER_HTTP_URL:-http://127.0.0.1:8080}"
curl -fsS "${base_url}/healthz"
curl -fsS "${base_url}/readyz"
curl -fsS -X POST "${base_url}/v1/addresses" -H 'content-type: application/json' -d '{"name":"verify.queue","kind":"queue","durable":true}'
curl -fsS -X POST "${base_url}/v1/messages" -H 'content-type: application/json' -d '{"address":"verify.queue","body":"verification"}'
curl -fsS "${base_url}/v1/addresses/verify.queue/depth"

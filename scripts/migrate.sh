#!/bin/sh
set -eu
: "${BROKER_POSTGRES_DSN:?BROKER_POSTGRES_DSN is required}"
for migration in migrations/*.sql; do
  echo "applying ${migration}"
  psql "${BROKER_POSTGRES_DSN}" -v ON_ERROR_STOP=1 -f "${migration}"
done

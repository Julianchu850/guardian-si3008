#!/usr/bin/env bash
# Escribe la IP pública actual de la EC2 en .env (KAFKA_ADVERTISED_HOST) y
# reinicia Kafka. Córrelo cada vez que se reinicie la sesión del Learner Lab.
set -euo pipefail
cd "$(dirname "$0")"
TOKEN=$(curl -sS -X PUT http://169.254.169.254/latest/api/token \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
IP=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/public-ipv4)
[ -f .env ] || cp .env.example .env
if grep -q '^KAFKA_ADVERTISED_HOST=' .env; then
  sed -i "s/^KAFKA_ADVERTISED_HOST=.*/KAFKA_ADVERTISED_HOST=$IP/" .env
else
  echo "KAFKA_ADVERTISED_HOST=$IP" >> .env
fi
echo "KAFKA_ADVERTISED_HOST=$IP"
docker compose up -d

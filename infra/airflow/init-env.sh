#!/usr/bin/env bash
# Crea .env con claves aleatorias y el UID del usuario actual. Córrelo una sola vez.
set -euo pipefail
cd "$(dirname "$0")"
if [ -f .env ]; then echo ".env ya existe; no lo toco"; exit 0; fi
cp .env.example .env
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
sed -i "s/^AIRFLOW_JWT_SECRET=.*/AIRFLOW_JWT_SECRET=$(openssl rand -hex 32)/" .env
sed -i "s/^AIRFLOW_ADMIN_PASSWORD=.*/AIRFLOW_ADMIN_PASSWORD=$(openssl rand -base64 12 | tr -d '/+=')/" .env
mkdir -p logs "$(grep '^GUARDIAN_DATA=' .env | cut -d= -f2)"
echo "Listo. Edita GUARDIAN_LAKE en .env. Clave de la UI:"
grep '^AIRFLOW_ADMIN_PASSWORD=' .env

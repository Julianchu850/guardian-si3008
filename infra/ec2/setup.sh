#!/usr/bin/env bash
# Prepara una EC2 con Amazon Linux 2023 para el proyecto.
# Se puede pegar como "User data" al lanzar la instancia, o correr con sudo.
set -euxo pipefail

dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user

# Plugins de Docker: compose y buildx (no vienen en Amazon Linux)
PLUGINS=/usr/local/lib/docker/cli-plugins
mkdir -p "$PLUGINS"
curl -fsSL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 \
  -o "$PLUGINS/docker-compose"
curl -fsSL https://github.com/docker/buildx/releases/download/v0.17.1/buildx-v0.17.1.linux-amd64 \
  -o "$PLUGINS/docker-buildx"
chmod +x "$PLUGINS/docker-compose" "$PLUGINS/docker-buildx"

# Código del proyecto
if [ ! -d /home/ec2-user/guardian-si3008 ]; then
  sudo -u ec2-user git clone https://github.com/Julianchu850/guardian-si3008.git \
    /home/ec2-user/guardian-si3008
fi

# Carpeta compartida entre el simulador (Kafka) y Airflow
sudo -u ec2-user mkdir -p /home/ec2-user/guardian-data/sim

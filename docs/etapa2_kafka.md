# Etapa 2: Kafka en EC2 (AWS Academy Learner Lab)

Resultado de esta etapa: el simulador publica eventos en Kafka y un consumidor los guarda en la capa bronce de S3, particionados por fecha y hora del evento.

```
simulador ──► Kafka (game-events, ad-events) ──► bronze-sink ──► s3://<bucket>/guardian/bronze/<tópico>/dt=…/hr=…/
                      └── interventions (lo usa el guardián en la etapa 4)
```

## 0. Antes de empezar

- Un bucket de S3 para el data lake. Puede ser uno que ya tengan del curso, con el prefijo `guardian/`.
- Si el repositorio es privado, la EC2 necesita un token para clonarlo (ver "Problemas frecuentes").

## 1. Lanzar la EC2

En la consola de EC2, **Launch instance**:

| Campo | Valor |
|---|---|
| AMI | Amazon Linux 2023 |
| Tipo | `t3.large` (2 vCPU, 8 GB). En la etapa 3 Airflow se suma en la misma máquina. Si el laboratorio no lo permite, `t3.medium` alcanza para esta etapa |
| Key pair | `vockey` (el `labsuser.pem` se descarga en *AWS Details* del laboratorio) |
| Almacenamiento | 30 GiB gp3 |
| Security group | Entrada 22, 9092 y 8080 **solo desde My IP**. Kafka no tiene autenticación: nunca lo abras a 0.0.0.0/0 |
| Advanced → IAM instance profile | `LabInstanceProfile` |
| Advanced → Metadata response hop limit | **2** (sin esto, los contenedores no obtienen las credenciales del rol) |
| Advanced → User data | El contenido de `infra/ec2/setup.sh` |

El script instala Docker, Docker Compose y clona el repositorio. Tarda unos 2 minutos después de que la instancia arranca.

## 2. Primer arranque

```bash
ssh -i labsuser.pem ec2-user@<ip-publica>
cd guardian-si3008/infra/kafka
cp .env.example .env
nano .env                 # BRONZE_URI=s3://<tu-bucket>/guardian
./set-host.sh             # escribe la IP pública en .env y levanta todo
docker compose ps
```

Debe quedar así:

| Servicio | Estado esperado |
|---|---|
| `kafka` | `running (healthy)` |
| `kafka-init` | `exited (0)`: creó los tópicos y terminó |
| `bronze-sink` | `running` |
| `kafka-ui` | `running` |

## 3. Generar eventos

Desde la EC2, con el simulador en un contenedor:

```bash
docker compose --profile sim run --rm simulator \
  --sink kafka --bootstrap kafka:29092 --out /out/stream-split-s7 \
  --players 200 --days 2 --speed 3600 --policy split --seed 7 --start-date 2026-11-23
```

`--out` deja las dimensiones (jugadores) y la verdad terreno de la corrida en `~/guardian-data/sim/`, para que Airflow las suba en la etapa 3. Usa una semilla y fechas distintas a las del histórico batch para que no se mezclen las corridas.

O desde tu portátil, con el puerto 9092 abierto para tu IP:

```bash
cd simulator
python -m guardian_sim --sink kafka --bootstrap <ip-publica>:9092 --players 200 --days 2 --speed 3600
```

`--speed 3600` reproduce una hora simulada por segundo: un día de juego tarda unos 24 segundos. Con `--speed 60` va a un minuto por segundo, mejor para una demo en vivo.

## 4. Verificar

```bash
# Mensajes en los tópicos: abre http://<ip-publica>:8080 (Kafka UI)

# Atraso del consumidor (LAG cerca de 0 = va al día)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 --describe --group bronze-sink

# Archivos en S3
aws s3 ls s3://<tu-bucket>/guardian/bronze/ --recursive | head

# Registro del consumidor
docker compose logs -f bronze-sink
```

El consumidor escribe un lote cada 5.000 mensajes o cada 60 segundos, lo que ocurra primero.

## 5. Cuando se reinicia la sesión del laboratorio

Al terminar las 4 horas la instancia se detiene; los contenedores y los datos de Kafka se conservan. Al volver:

1. Inicia la instancia en la consola.
2. Entra por SSH (la IP pública cambió).
3. Corre `./set-host.sh` en `infra/kafka`.

## Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `bronze-sink` falla con `NoCredentialsError` | Falta el hop limit en 2. Desde la consola: *Actions → Instance settings → Modify instance metadata options*, o `aws ec2 modify-instance-metadata-options --instance-id <id> --http-put-response-hop-limit 2 --http-endpoint enabled` |
| `AccessDenied` al escribir en S3 | El bucket es de otra cuenta o el nombre está mal en `BRONZE_URI` |
| El portátil conecta pero se queda esperando | `KAFKA_ADVERTISED_HOST` tiene una IP vieja: corre `./set-host.sh` |
| No conecta nada al 9092 o al 8080 | El security group no tiene tu IP actual |
| `git clone` pide usuario en el user data | El repositorio es privado: clona a mano con `git clone https://<token>@github.com/Julianchu850/guardian-si3008.git` |

## Decisiones de diseño (para el informe)

- **KRaft, un broker, factor de replicación 1.** Suficiente para el laboratorio. En producción serían 3 brokers con replicación 3.
- **Clave del mensaje = `player_id`.** Todos los eventos de un jugador van a la misma partición y llegan en orden, que es lo que necesitan las ventanas por jugador del guardián.
- **Particiones:** `game-events` 6, `ad-events` 3, `interventions` 3. Permiten varios consumidores en paralelo dentro de un grupo.
- **Al menos una vez.** Los offsets se confirman después de escribir en S3. Si algo falla se reprocesa, y los duplicados se eliminan en la capa plata por `event_id`.
- **Partición por hora del evento**, no de llegada. Los eventos atrasados quedan en su día correcto.
- **Retención de 7 días en Kafka.** El histórico completo vive en S3; Kafka es el buffer.
- **Mensajes ilegibles** van a `bronze/_dead_letter/` en vez de detener el consumidor.

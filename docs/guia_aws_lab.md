# Guía paso a paso en AWS Academy Learner Lab

Qué crear en la consola, en qué orden, y cómo correr el pipeline. Todo en la región **us-east-1 (N. Virginia)**.

```
                         ┌──────────── EC2 guardian-host (t3.large) ────────────┐
 simulador ──► Kafka ──► │ bronze-sink ─────────────┐                           │
                         │ Airflow: carga batch ────┼──► S3  bronze/ batch/ truth/
                         │ Airflow: ETL ── crea ──┐ │                           │
                         └────────────────────────┼─┼───────────────────────────┘
                                                  ▼ │
                                   EMR (temporal): Spark bronce → plata → oro
                                                  │
                                   Glue Data Catalog ──► Athena (consultas, dashboard)
```

| Recurso | Cuántos | Quién lo crea |
|---|---|---|
| Bucket S3 | 1 | Ustedes, una vez (paso A2) |
| Security group | 1 | Ustedes, una vez (paso A4) |
| EC2 `guardian-host` | 1 | Ustedes, una vez (paso A5) |
| Clúster EMR | 1 por ejecución del ETL | **Airflow**, y lo apaga al terminar |
| Base de datos y tablas de Glue | 1 base, 11 tablas | **Spark**, automáticamente |
| Roles IAM | Los del laboratorio | Ya existen, solo se verifican (paso A3) |

---

## Parte A. Crear los recursos (una sola vez)

### A1. Iniciar el laboratorio

1. En AWS Academy, abre el Learner Lab y pulsa **Start Lab**. Espera a que el círculo quede verde.
2. Pulsa **AWS** para abrir la consola.
3. Arriba a la derecha, confirma la región **N. Virginia (us-east-1)**.
4. En **AWS Details** descarga `labsuser.pem` (la llave para entrar por SSH).

### A2. Bucket de S3

**S3 → Create bucket**

| Campo | Valor |
|---|---|
| Bucket type | General purpose |
| Name | Único en todo AWS, por ejemplo `guardian-lake-jl-2026`. También pueden reutilizar el bucket de los laboratorios del curso |
| Block all public access | Activado (por defecto) |
| Lo demás | Por defecto |

No hace falta crear carpetas: el pipeline escribe todo bajo el prefijo `guardian/`:

```
s3://<bucket>/guardian/
├── bronze/          eventos crudos (Kafka y batch)
├── batch/           dimensiones: jugadores, catálogo
├── truth/           verdad terreno (solo evaluación)
├── silver/          datos limpios en Parquet
├── gold/            métricas para el dashboard
├── quality/         reportes de calidad
├── code/            jobs de Spark que sube Airflow
├── emr-logs/        logs de EMR
└── athena-results/  resultados de Athena
```

### A3. Verificar los roles

**IAM → Roles**, busca estos tres. Deben existir; no los crees ni los modifiques.

| Rol | Para qué |
|---|---|
| `LabRole` | Permisos de la EC2 (vía `LabInstanceProfile`): S3, EMR, Glue |
| `EMR_DefaultRole` | Rol de servicio de EMR |
| `EMR_EC2_DefaultRole` | Rol de las máquinas del clúster EMR |

Si faltan los de EMR, abre **CloudShell** (icono `>_` arriba) y corre:

```bash
aws emr create-default-roles
```

### A4. Security group

**EC2 → Security Groups → Create security group**

- Name: `guardian-sg`
- VPC: la que viene por defecto.
- Inbound rules, todas con **Source = My IP**:

| Puerto | Para qué |
|---|---|
| 22 | SSH |
| 8080 | Kafka UI |
| 8081 | Airflow |
| 9092 | Kafka, para publicar desde su portátil (opcional) |

> Nunca pongan `0.0.0.0/0`: Kafka no tiene contraseña. Si cambian de red (casa, universidad), editen las reglas con la nueva *My IP*.

### A5. Instancia EC2

**EC2 → Launch instance**

| Campo | Valor |
|---|---|
| Name | `guardian-host` |
| AMI | Amazon Linux 2023 |
| Instance type | `t3.large` (8 GB: Kafka y Airflow juntos). Si el laboratorio no lo permite, usen el más grande disponible; con 4 GB Kafka y Airflow pueden quedarse cortos |
| Key pair | `vockey` |
| Network settings → Select existing security group | `guardian-sg` |
| Storage | 30 GiB gp3 |
| Advanced details → IAM instance profile | `LabInstanceProfile` |
| Advanced details → Metadata response hop limit | **2** |
| Advanced details → User data | Pega el contenido de `infra/ec2/setup.sh` |

Pulsa **Launch instance** y copia la **Public IPv4 address**.

> El *hop limit* en 2 es lo que permite que los contenedores (bronze-sink y Airflow) usen las credenciales del rol. Es el error más común si se olvida.

### A6. Athena

**Athena → Query editor → Settings → Manage**

- Query result location: `s3://<bucket>/guardian/athena-results/`
- Save.

---

## Parte B. Configurar la EC2 (una sola vez)

### B1. Entrar por SSH

```bash
chmod 400 labsuser.pem
ssh -i labsuser.pem ec2-user@<ip-publica>
docker compose version            # si responde, el user data terminó
ls ~/guardian-si3008              # el repositorio ya está clonado
```

Si `docker compose` no existe, el user data no terminó o falló. Espera un par de minutos, o córrelo a mano:

```bash
curl -fsSL https://raw.githubusercontent.com/Julianchu850/guardian-si3008/main/infra/ec2/setup.sh | sudo bash
exit      # vuelve a entrar para que tome el grupo docker
```

### B2. Kafka (etapa 2)

```bash
cd ~/guardian-si3008/infra/kafka
cp .env.example .env
nano .env        # BRONZE_URI=s3://<bucket>/guardian
./set-host.sh    # escribe la IP pública y levanta Kafka, Kafka UI y bronze-sink
docker compose ps
```

Detalle y verificación: [etapa2_kafka.md](etapa2_kafka.md).

### B3. Airflow (etapa 3)

```bash
cd ~/guardian-si3008/infra/airflow
./init-env.sh    # crea .env con claves aleatorias y muestra la clave de la UI
nano .env        # GUARDIAN_LAKE=s3://<bucket>/guardian  (mismo bucket que en Kafka)
docker compose up -d --build      # la primera vez tarda ~5 minutos
docker compose ps
```

En `.env` revisen también la configuración de EMR. Usen la misma versión y tipo de máquina que les funcionaron en los laboratorios del curso:

| Variable | Por defecto | Nota |
|---|---|---|
| `EMR_RELEASE` | `emr-7.2.0` | Spark 3.5 |
| `EMR_INSTANCE_TYPE` | `m5.xlarge` | Si da error de tipo no permitido, prueben `m4.xlarge` o `m4.large` |
| `EMR_CORE_COUNT` | `2` | Nodos de trabajo además del primario |
| `EMR_SUBNET` | vacío | Solo si EMR pide una subred: usen la misma de la EC2 |
| `EMR_KEY` | `vockey` | Permite entrar por SSH al clúster si hay que depurar |

Abre **http://\<ip-publica\>:8081**, entra con `admin` y la clave que mostró `init-env.sh` (está en `.env`). Deben aparecer dos DAGs pausados: `guardian_carga_batch` y `guardian_etl`. Actívalos con el interruptor.

---

## Parte C. Correr el pipeline (cada vez que lo quieran mostrar)

### C1. Streaming: simulador → Kafka → S3 bronce

```bash
cd ~/guardian-si3008/infra/kafka
docker compose --profile sim run --rm simulator \
  --sink kafka --bootstrap kafka:29092 --out /out/stream-split-s7 \
  --players 200 --days 2 --speed 3600 --policy split --seed 7 --start-date 2026-11-23
```

Mientras corre, muestren los mensajes en Kafka UI (`:8080`) y los archivos que aparecen en S3 en `guardian/bronze/`.

### C2. Batch: histórico y dimensiones → S3

Airflow → `guardian_carga_batch` → **Trigger** (botón ▶) con los parámetros:

| Parámetro | Valor sugerido |
|---|---|
| generar_historico | true |
| jugadores | 300 |
| dias | 7 |
| politica | split |
| semilla | 42 |
| fecha_inicio | 2026-11-16 |

Genera una semana de juego (unos 30 segundos) y sube a S3 los eventos históricos, los jugadores, el catálogo y la verdad terreno. También sube las dimensiones de las corridas por Kafka del paso C1.

### C3. ETL en EMR: bronce → plata → oro

Airflow → `guardian_etl` → **Trigger** con `fechas = auto`. Detecta solo las fechas con datos nuevos.

| Momento | Qué se ve | Duración aprox. |
|---|---|---|
| `crear_emr` | En la consola de EMR, el clúster en *Starting* | 7-10 min |
| `esperar_bronce_plata` | Paso 1 *Running* en la pestaña Steps | 3-6 min |
| `esperar_plata_oro` | Paso 2 *Running* | 2-4 min |
| `apagar_emr` | Clúster *Terminating → Terminated* | 1-2 min |
| `validar_calidad` | Verde si menos del 1 % de registros son inválidos | segundos |

### C4. Ver los resultados en Athena

```sql
-- Tablas que creó Spark en el catálogo
SHOW TABLES IN guardian;

-- Dos mundos: ingresos vs. daño por política y día
SELECT dt, policy, players, round(ad_revenue_usd, 2) AS anuncios_usd,
       round(purchases_usd, 2) AS compras_usd, round(ads_per_hour, 1) AS anuncios_hora,
       round(loss_after_record_rate, 3) AS derrotas_tras_record,
       round(revenue_top5_share, 3) AS ingreso_top5
FROM guardian.gold_harm_daily
ORDER BY dt, policy;

-- Repartos forzados por regla (verdad terreno, auditoría del experimento)
SELECT policy, rule, sum(forced) AS forzados
FROM guardian.gold_forced_deals_daily
GROUP BY policy, rule ORDER BY forzados DESC;
```

### C5. El guardián (etapa 4)

1. **Entrenar.** Airflow → `guardian_entrenamiento` → Trigger. Usa las variables que dejó el ETL en `gold/deal_features`. Si el modelo pasa la validación, queda en `s3://<bucket>/guardian/models/guardian/latest/`.
2. **Servicio en streaming:**
   ```bash
   cd ~/guardian-si3008/infra/kafka
   nano .env        # MODEL_URI=s3://<bucket>/guardian/models/guardian/latest
   docker compose --profile guardian up -d --build guardian-scorer
   ```
3. **Tercer mundo:** `guardian_carga_batch` con `politica = guarded`, y después `guardian_etl`. En Athena, `gold_harm_daily` muestra las tres políticas.

Detalle y resultados: [etapa4_guardian.md](etapa4_guardian.md).

### C6. Antes de cerrar

- **EMR → Clusters:** que no quede ninguno en *Waiting* o *Running*. Airflow los apaga, y además se apagan solos tras 1 hora ociosos, pero revisen.
- La EC2 se detiene sola al terminar la sesión; no hace falta borrarla.

---

## Parte D. Al volver en otra sesión

1. **Start Lab** y abre la consola.
2. **EC2 → Instances →** `guardian-host` → **Start instance**. La IP pública cambia.
3. Si cambiaron de red, actualicen *My IP* en `guardian-sg`.
4. Entra por SSH y corre:

```bash
cd ~/guardian-si3008 && git pull
cd infra/kafka && ./set-host.sh
cd ../airflow && docker compose up -d
```

---

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| `NoCredentialsError` en bronze-sink o en tareas de Airflow | Hop limit en 2: *EC2 → Actions → Instance settings → Modify instance metadata options* |
| `crear_emr` falla con un tipo de instancia no soportado | Cambien `EMR_INSTANCE_TYPE` en `infra/airflow/.env` y corran `docker compose up -d` |
| `crear_emr` falla por `iam:PassRole` o rol inválido | Confirmen los nombres de los roles en IAM (paso A3) |
| `crear_emr` pide una subred | Pongan en `EMR_SUBNET` la subred de la EC2 (pestaña *Networking* de la instancia) |
| Un paso de EMR queda en *Failed* | EMR → el clúster → Steps → *stderr* del paso. También en `s3://<bucket>/guardian/emr-logs/` |
| `validar_calidad` falla | El mensaje dice qué fecha y por qué. Los registros malos quedan en `silver/_quarantine/` |
| La UI de Airflow no carga | `docker compose logs airflow-apiserver`. Si dice memoria insuficiente, la instancia es muy pequeña |
| `git clone` falló en el user data | El repositorio es privado: clónenlo a mano con un token |

## Costo aproximado

Precios de referencia en us-east-1; revisen el saldo en el panel del laboratorio.

| Recurso | Costo | Uso típico |
|---|---|---|
| EC2 `t3.large` | ~US$0,08/h | Solo mientras la sesión está activa |
| EMR, 3 × `m5.xlarge` | ~US$0,72/h | ~20-30 min por ETL ≈ US$0,30 |
| S3, Glue, Athena | Centavos | Con estos volúmenes |

# Guardián

Sistema de datos intensivos que simula un juego tipo Block Blast con monetización depredadora, mide el daño a los jugadores y detecta la manipulación en tiempo real.

Proyecto final de SI3008 Sistemas Intensivos en Datos (EAFIT). Stack exigido: **Kafka** para streaming y **Airflow** para orquestación, sobre AWS (S3 y EMR).

**Para montarlo en AWS: [guía paso a paso del Learner Lab](docs/guia_aws_lab.md).**

Caso de referencia: Omolewa, A. (2026). *How Block Blast harvests its players for ad money*. https://ayoships.com/breakdowns/block-blast

## Etapas

| Etapa | Contenido | Estado |
|---|---|---|
| **1. Simulador** | Motor del juego, políticas, agentes con perfiles, eventos | ✅ Lista |
| **2. Ingesta streaming** | Kafka (KRaft) en Docker sobre EC2, productor y consumidor a S3 bronce | ✅ Lista: [guía](docs/etapa2_kafka.md) |
| **3. Batch y orquestación** | Airflow 3 en Docker: carga batch a S3 y ETL en EMR/Spark bronce → plata → oro, con calidad y catálogo Glue | ✅ Lista |
| **4. Guardián (ML)** | Variables por reparto, modelo de riesgo, servicio de streaming sobre Kafka y modo embebido (tercer mundo) | ✅ Lista: [detalle](docs/etapa4_guardian.md) |
| 5. Dashboard | Comparación "dos mundos" sobre la capa oro (Athena) | Pendiente |
| 6. Juego web (opcional) | FastAPI + navegador en EC2 para jugadores reales (`source = human`) | Pendiente |

## Estructura

```
guardian-si3008/
├── docs/
│   ├── guia_aws_lab.md          # qué crear en el Learner Lab y cómo correr todo
│   ├── esquema_eventos.md       # contrato de datos: eventos, tópicos, verdad terreno
│   ├── etapa2_kafka.md          # detalle de Kafka en EC2
│   └── etapa4_guardian.md       # el guardián: variables, modelo, resultados
├── infra/
│   ├── ec2/setup.sh             # instala Docker y clona el repo en Amazon Linux 2023
│   ├── kafka/                   # docker-compose: Kafka, tópicos, Kafka UI, bronze-sink, simulador
│   └── airflow/                 # docker-compose de Airflow 3 y DAGs (carga batch, ETL en EMR)
├── ingestion/bronze_sink/       # consumidor Kafka → S3 bronce (al menos una vez)
├── spark/jobs/                  # PySpark: bronce → plata → oro y variables del guardián (EMR 7, Spark 3.5)
├── guardian_ml/                 # el guardián: variables, entrenamiento, servicio de streaming
└── simulator/
    ├── guardian_sim/
    │   ├── engine.py            # tablero 8x8 (bitboard), piezas, colocación, solver
    │   ├── dealer.py            # políticas neutral y depredadora, mano letal
    │   ├── ads.py               # mercado de anuncios (eCPM por país/dispositivo)
    │   ├── profiles.py          # perfiles y rasgos latentes
    │   ├── agent.py             # cómo juegan y deciden los agentes
    │   ├── simulation.py        # sesiones, partidas, eventos y etiquetas
    │   ├── sinks.py             # salida a archivos (estructura del data lake) o Kafka
    │   ├── report.py            # reporte de validación
    │   └── __main__.py          # línea de comandos
    └── tests/
```

## Uso

Requiere Python 3.11 o superior.

```bash
cd simulator
pip install -r requirements.txt

# Pruebas
python -m pytest

# Una semana con 400 jugadores, el mismo mundo en las dos políticas
python -m guardian_sim --players 400 --days 7 --policy neutral   --out ../data/neutral
python -m guardian_sim --players 400 --days 7 --policy predatory --out ../data/predatory
python -m guardian_sim.report ../data/neutral ../data/predatory

# Experimento A/B: mitad de cada perfil en cada política
python -m guardian_sim --players 400 --days 7 --policy split --out ../data/ab

# Publicar en Kafka (etapa 2), 1 hora simulada por segundo
python -m guardian_sim --sink kafka --bootstrap <ip-ec2>:9092 --speed 3600
```

Con la misma semilla, los dos mundos tienen exactamente los mismos jugadores; solo cambia la política. Eso es lo que permite atribuir las diferencias a la manipulación.

## Capas del data lake

| Capa | Contenido | Formato |
|---|---|---|
| Bronce | Eventos tal como llegan de Kafka o del histórico batch, por fecha y hora del evento | JSONL (gzip) |
| Plata | Eventos validados, tipados y sin duplicados; dimensiones; verdad terreno. Los inválidos quedan en `_quarantine/` | Parquet por `dt` |
| Oro | `player_daily`, `harm_daily` (dos mundos) y `forced_deals_daily` (auditoría) | Parquet por `dt` |

Las tablas de plata y oro quedan registradas en el Glue Data Catalog (base `guardian`) y se consultan con Athena.

Para probar el ETL sin AWS, con PySpark local:

```bash
pip install pyspark==3.5.7
cd spark/jobs
spark-submit --master "local[2]" --py-files common.py bronze_to_silver.py --lake ../../data/ab --dates 2026-11-16 --dims
spark-submit --master "local[2]" --py-files common.py silver_to_gold.py  --lake ../../data/ab --dates 2026-11-16
```

## Cómo funciona el simulador

**Motor.** El tablero es un entero de 64 bits. Colocar piezas, limpiar líneas y medir huecos son operaciones de bits, así que una semana de 400 jugadores se simula en unos 35 segundos con 2 núcleos.

**Repartidor.** La política neutral reparte manos al azar pero siempre con solución, y la dificultad sube igual para todos a lo largo de la partida. La política depredadora agrega tres reglas abstraídas del caso de referencia:

- `ad_harvest`: si los últimos 3 anuncios rewarded se vieron completos, el anuncio cargado vale al menos 3,5 veces el anterior y el puntaje supera el récord más un delta (500, 750 o 1.000), reparte una mano letal.
- `record_margin`: superado el récord en un 10 %, cada reparto tiene un 20 % de probabilidad de ser letal.
- `fast_hand`: si el jugador va rápido y cerca del récord, reparte piezas grandes.

Una mano letal tiene 2 piezas que caben y 1 que no, y el solver exhaustivo confirma que ningún orden ni posición permite colocar las tres.

**Agentes.** Todos usan la misma heurística: evalúan las jugadas posibles (líneas, huecos, espacio para un 3x3, fragmentación, si las piezas restantes seguirán cabiendo) y eligen con una softmax. El perfil cambia la temperatura (errores), cuántas jugadas revisan, los pesos de estilo y la velocidad. La frustración acumulada empeora y acelera el juego. Revivir, ver anuncios, comprar, seguir jugando y volver al día siguiente son decisiones probabilísticas según el perfil y el contexto.

**Perfiles:** casual (45 %), competitivo (20 %), gastador (17 %), persigue pérdidas (13 %) y "ballena" (5 %).

## Primeros resultados (400 jugadores, 7 días, semilla 42)

| | Neutral | Depredadora |
|---|---|---|
| Repartos forzados por 100 partidas | 0,0 | 2,9 |
| Anuncios por hora de juego | 28,2 | 32,7 |
| Ingreso por anuncios (USD) | 115 | 154 (+34 %) |
| Compras (USD) | 148 | 177 (+20 %) |
| Días activos promedio | 4,44 | 4,46 |

Las reglas golpean más a los casuales (5,7 repartos forzados por 100 partidas): su récord es bajo y lo superan seguido. Los valores de anuncios y comportamiento son supuestos de calibración, no datos reales.

# Etapa 5: dashboard "tres mundos"

Un dashboard en Streamlit que consulta la base `guardian` en Athena y compara el mismo juego en tres mundos: **neutral**, **depredador** y **con guardián**.

```
Glue Data Catalog (base guardian) ──► Athena ──► Streamlit (EC2, puerto 8501)
S3 models/guardian/latest/metrics.json ──────────┘
```

## Qué muestra

| Sección | Pregunta que responde | Fuente |
|---|---|---|
| Resumen | Para cada mundo: ingreso por anuncios por jugador, anuncios por hora, manos letales por 100 partidas, derrotas tras superar el récord | `gold_player_daily`, `silver_deal_labels` |
| Ingreso vs. daño | Lo que gana el estudio frente a lo que sufre el jugador, lado a lado | Igual |
| Día a día | Si las diferencias se sostienen todos los días | `gold_player_daily` |
| Manipulación | Qué regla provocó las manos letales y cuántas bloqueó el guardián | `silver_deal_labels` |
| ¿A quién golpea? | Qué perfiles reciben más manipulación y aportan más ingreso | Verdad terreno: `silver_players_truth` |
| El guardián | Métricas del modelo frente a la regla escrita a mano, importancia de variables e intervenciones (embebido y streaming) | `metrics.json` en S3, `silver_game_events`, `bronze_interventions` |
| Estado del data lake | Filas y fechas de cada tabla | Todas |

**Filtros:**
- **Población (semilla):** con la misma semilla, los tres mundos tienen exactamente los mismos jugadores.
- **Fechas.**
- **Mundos.**

**Accesibilidad:** cada gráfico tiene su tabla de datos en *Ver datos*. La paleta (azul claro, naranja, azul marino) está validada para daltonismo en todos los pares y tiene contraste mínimo de 3:1 sobre el fondo. El color de cada mundo es el mismo en todos los gráficos.

Al arrancar, el dashboard crea en Athena la tabla `bronze_interventions` sobre `bronze/interventions/`, con *partition projection*: no hace falta correr crawlers ni `MSCK REPAIR`.

## Paso 1. Datos limpios para comparar los tres mundos

Para que el dashboard compare exactamente los mismos jugadores, carguen los tres mundos con la misma semilla. En Airflow → `guardian_carga_batch` corran tres veces, cambiando solo `politica`:

| jugadores | dias | politica | semilla | fecha_inicio |
|---|---|---|---|---|
| 300 | 7 | `neutral` | 42 | 2026-11-16 |
| 300 | 7 | `predatory` | 42 | 2026-11-16 |
| 300 | 7 | `guarded` | 42 | 2026-11-16 (ya lo corrieron en la etapa 4) |

Después corran `guardian_etl` una sola vez, con `fechas = auto`.

> El histórico `split` de la etapa 3 (semilla 42) no estorba. El simulador es determinista: cada jugador de esa corrida genera exactamente los mismos eventos en la corrida completa de su mundo, y la capa plata elimina los duplicados por `event_id`.

## Paso 2. Abrir el puerto

En el security group `guardian-sg` agreguen una regla de entrada: **Custom TCP 8501**, Source **My IP**.

## Paso 3. Levantar el dashboard

```bash
cd ~/guardian-si3008 && git pull
cd infra/dashboard
cp .env.example .env
nano .env        # GUARDIAN_LAKE=s3://<bucket>/guardian
docker compose up -d --build
docker compose logs -f dashboard
```

Abran **http://\<ip-publica\>:8501**. La primera carga tarda unos segundos porque cada sección es una consulta a Athena; luego se guardan en caché por 5 minutos.

En el selector de población elijan **p42**: es la que tiene los tres mundos con los mismos jugadores.

## Probarlo sin AWS

El mismo dashboard corre contra una carpeta local del data lake, con DuckDB:

```bash
cd dashboard
pip install -r requirements.txt
GUARDIAN_BACKEND=duckdb GUARDIAN_LAKE=../data/lake PYTHONPATH=. streamlit run guardian_dashboard/app.py
```

La carpeta debe tener `silver/` y `gold/` (los jobs de Spark en modo local las generan) y, opcionalmente, `model-latest/metrics.json`.

## Consultas para el informe

`sql/athena/tres_mundos.sql` tiene las consultas de las cifras principales: tres mundos, manos letales por regla, perfiles afectados, concentración del ingreso e intervenciones. Ojo: en Athena la columna `dt` es de tipo `DATE`. Compárenla con `DATE '2026-11-16'`, no con texto.

# Etapa 4: el guardián

El guardián estima, con lo que se observa justo antes de cada reparto, la probabilidad de que el juego esté a punto de manipular la partida (repartir una mano imposible). Cuando el riesgo supera un umbral, interviene.

## Cómo funciona

```
                  ┌──────────── mismas variables + mismo modelo ────────────┐
                  │                                                         │
 Modo embebido    │  simulador / backend ─► pre_deal() ─► ¿bloquear? ─► reparte
 (preventivo)     │                                                         │
                  │                                                         │
 Modo streaming   │  Kafka game-events + ad-events ─► guardian-scorer ─► Kafka interventions
 (monitoreo)      │        (reordena por secuencia)        │                      │
                  └────────────────────────────────────────┼──────────────────────┘
                                                           ▼
                                               bronze-sink ─► S3 bronze/interventions/

 Entrenamiento:  plata ─► Spark deal_features ─► gold/deal_features ─► Airflow entrena ─► S3 models/guardian/latest/
```

- **Modo embebido.** Decide antes de repartir, que es donde se puede evitar la manipulación. Es como funcionaría dentro del backend del juego. Se usa en el simulador con la política `guarded` (el tercer mundo).
- **Modo streaming.** El servicio `guardian-scorer` lee Kafka, puntúa cada reparto y publica intervenciones en el tópico `interventions` para los repartos siguientes del jugador. Sirve para monitorear y auditar en tiempo real, sin tocar el código del juego.

Los dos usan la misma clase (`guardian_ml/guardian.py`), las mismas variables y el mismo modelo.

## Variables (15)

Todas son observables por un auditor externo y se calculan solo con eventos anteriores al reparto. No usan la verdad terreno ni los rasgos latentes del jugador.

| Variable | Qué mide |
|---|---|
| `score`, `high_score`, `score_ratio`, `above_record`, `record_margin` | Qué tan cerca o por encima de su récord va el jugador |
| `loaded_ecpm`, `last_rewarded_ecpm`, `ecpm_ratio` | Cuánto vale el anuncio cargado frente al último que vio |
| `rewarded_last3_completed`, `rewarded_count` | Su historial de anuncios con recompensa |
| `deal_num`, `game_num`, `revives_in_game` | En qué punto de la partida y de su historia está |
| `avg_turn_s_last6` | Qué tan rápido juega |
| `board_fill` | Qué tan lleno está el tablero |

La definición vive en `guardian_ml/features.py`. El job `spark/jobs/deal_features.py` la reproduce con ventanas de Spark para el histórico. Una prueba verifica que ambos den exactamente lo mismo: en 65.585 repartos no hubo ninguna diferencia. Si no coincidieran, el modelo se entrenaría con datos distintos a los que ve en producción.

## Modelo

- **Algoritmo:** gradient boosting (`HistGradientBoostingClassifier` de scikit-learn).
- **Datos:** repartos del mundo depredador (en el neutral no hay manipulación que aprender). Etiqueta: `forced` de la verdad terreno.
- **Validación temporal:** entrena con los primeros días y evalúa con los últimos, como se usaría en producción.
- **Umbral:** el más alto que logra un recall del 90 % en una validación dentro del entrenamiento. El conjunto de prueba no se usa para elegirlo.

Resultados con 800 jugadores × 10 días (528.816 repartos, 563 forzados). Prueba: 192.631 repartos de los últimos 5 días, con 30 forzados.

| | Precisión | Recall | Alertas (% de repartos) | PR-AUC |
|---|---|---|---|---|
| Modelo | 0,21 | 0,87 | 0,07 % | 0,52 |
| Regla escrita a mano (las condiciones del artículo) | 0,04 | 1,00 | 0,44 % | — |

Con un recall parecido, el modelo es unas 6 veces más preciso que aplicar las reglas a mano. Las variables más importantes fueron `above_record` y `ecpm_ratio`, seguidas del puntaje frente al récord y el historial de anuncios: el modelo redescubrió las condiciones que describe el caso de referencia. También aprendió algo que la regla no dice: la mano letal solo se puede armar con el tablero a medio llenar (`board_fill`).

La precisión es baja en términos absolutos porque una de las reglas depredadoras es aleatoria: superado el récord en 10 %, cada reparto tiene un 20 % de probabilidad de ser letal. El guardián detecta el contexto de riesgo, pero no puede saber cuál de esos repartos será el forzado. Una falsa alarma cuesta poco: bloquear una manipulación que no iba a ocurrir no cambia nada para el jugador.

El conjunto de prueba tiene solo 30 positivos, así que las métricas tienen bastante incertidumbre. Para el informe conviene entrenar con más jugadores o más días.

## Intervenciones

| Acción | Cuándo | Efecto |
|---|---|---|
| `block_manipulation` | Riesgo ≥ umbral del modelo | El juego no aplica reglas depredadoras el resto de esa partida |
| `suggest_break` | 2 derrotas en la sesión justo después de superar el récord | Se sugiere una pausa |
| `spend_limit` | 2 compras en la misma sesión | Se bloquean compras el resto de la sesión |

Las dos últimas son reglas simples sobre patrones de daño, no salidas del modelo.

## Tres mundos (400 jugadores, 7 días, semilla 42)

El modelo se entrenó con otra población (semilla 11), así que esta es una evaluación con jugadores que nunca vio.

| | Neutral | Depredador | Con guardián |
|---|---|---|---|
| Manos letales por 100 partidas | 0,0 | 2,9 | **0,1** |
| Derrotas tras superar el récord | 10,0 % | 10,9 % | **9,9 %** |
| Anuncios por hora | 28,2 | 32,7 | 32,8 |
| Ingreso por anuncios (USD) | 115 | 154 | 148 |
| Compras (USD) | 148 | 177 | 191 |
| Días activos por jugador | 4,44 | 4,46 | 4,49 |

- El guardián suprimió 1.181 intentos de manipulación; se le escaparon 9 de unos 245.
- Bloqueó la manipulación en 303 de 8.484 partidas (3,6 %).
- Las derrotas tras superar el récord bajaron al nivel del juego neutral.
- El ingreso por anuncios bajó solo un 4 %. Para el estudio, dejar de manipular cuesta poco.
- Las compras son pocas por jugador y varían mucho con el azar; no hay que leer la diferencia en compras como un efecto del guardián.

**En streaming** (la misma semana del mundo depredador, reproducida por el servicio con los dos tópicos intercalados desordenadamente):

- Procesó unos 120.000 eventos por segundo.
- De los 245 repartos forzados, 170 cayeron en partidas que ya tenía alertadas y 59 los detectó en ese mismo reparto. Solo 16 (7 %) pasaron sin alerta.

## Correrlo en AWS

1. **Variables:** el ETL (`guardian_etl`) ahora tiene un tercer paso de Spark que escribe `gold/deal_features`.
2. **Entrenar:** en Airflow → `guardian_entrenamiento` → Trigger. Si el modelo pasa la compuerta (PR-AUC ≥ 0,3 y recall ≥ 0,7 en prueba), queda en `s3://<bucket>/guardian/models/guardian/latest/`.
3. **Servicio de streaming:**
   ```bash
   cd ~/guardian-si3008/infra/kafka
   nano .env        # MODEL_URI=s3://<bucket>/guardian/models/guardian/latest
   docker compose --profile guardian up -d --build guardian-scorer
   docker compose logs -f guardian-scorer
   ```
4. **Tercer mundo en vivo:**
   ```bash
   docker compose --profile sim run --rm simulator --sink kafka --bootstrap kafka:29092 \
     --policy guarded --guardian-model s3://<bucket>/guardian/models/guardian/latest \
     --out /out/stream-guarded-s9 --players 100 --days 1 --seed 9 --start-date 2026-12-01
   ```
   En Kafka UI aparecen las intervenciones en el tópico `interventions`.
5. **Tercer mundo en batch:** `guardian_carga_batch` con `politica = guarded`, y luego `guardian_etl`. En Athena, `gold_harm_daily` muestra las tres políticas.

## Limitaciones (para el informe)

- **Datos simulados.** Las reglas depredadoras son una abstracción del caso de referencia y el comportamiento de los agentes es un supuesto calibrado a mano.
- **Pocos positivos.** La manipulación es rara (cerca del 0,1 % de los repartos), así que las métricas varían bastante entre corridas.
- **Límites del modo streaming.** Actúa sobre los repartos siguientes, no sobre el que ya se repartió. La prevención completa requiere que el guardián esté dentro del backend del juego (modo embebido).
- **El modelo aprende las reglas de este simulador.** Frente a otras formas de manipulación habría que reentrenarlo, y el DAG de entrenamiento existe justamente para eso.

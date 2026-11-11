# Esquema de datos (v1)

## Dónde vive cada dato

| Dato | Viaja por Kafka | Destino en el data lake | Lo puede usar el guardián |
|---|---|---|---|
| Eventos de juego | Sí, tópico `game-events` | `bronze/game-events/dt=AAAA-MM-DD/hr=HH/` | Sí |
| Eventos de anuncios y tienda | Sí, tópico `ad-events` | `bronze/ad-events/dt=…/hr=…/` | Sí |
| Jugadores (datos observables) | No, carga batch | `batch/players/players.jsonl` | Sí |
| Catálogo de piezas y ofertas | No, carga batch | `batch/catalog/` | Sí |
| Verdad terreno de jugadores | No | `truth/players_truth.jsonl` | **No**, solo evaluación |
| Verdad terreno de repartos | No | `truth/deal_labels/dt=…/` | **No como variable**; es la etiqueta |

La clave de cada mensaje en Kafka es `player_id`, para que los eventos de un mismo jugador caigan en la misma partición y lleguen en orden.

## Campos comunes de todos los eventos

| Campo | Tipo | Descripción |
|---|---|---|
| `event_id` | string | Único: `p00042-1234` |
| `event_type` | string | Ver tabla siguiente |
| `ts` | string ISO-8601 UTC | Hora simulada del evento |
| `player_id` | string | Jugador anónimo |
| `session_id` | string | `p00042-s7` |
| `game_id` | string o null | `p00042-g31`; null fuera de una partida |
| `policy` | string | `neutral` o `predatory` (brazo del experimento) |
| `source` | string | `sim` o `human` (juego web, etapa 6) |
| `schema_version` | int | 1 |

## Tipos de evento

| `event_type` | Tópico | Campos propios |
|---|---|---|
| `session_start` | game | `country`, `platform`, `device_tier`, `local_hour` |
| `game_start` | game | `game_num`, `high_score` |
| `piece_deal` | game | `deal_id`, `deal_num`, `pieces` (ids), `board` (hex de 64 bits), `placeable_now`, `score`, `high_score`, `rewarded_ecpm_loaded` |
| `turn_result` | game | `deal_id`, `pieces_placed`, `lines_cleared`, `points`, `score`, `turn_ms` |
| `game_over` | game | `deal_id`, `score`, `high_score`, `above_record` |
| `revive_offer` | game | `revives_used`, `rewarded_ecpm_loaded` |
| `revive_response` | game | `accepted` |
| `revive` | game | `revives_used`, `board` |
| `game_end` | game | `final_score`, `high_score`, `new_record`, `deals`, `revives_used` |
| `intervention_applied` | game | `action` |
| `session_end` | game | `games`, `duration_s` |
| `ad_impression` | ads | `ad_type` (rewarded/interstitial), `placement`, `ecpm`, `revenue_usd`, `completed`, `watch_ms` |
| `reward_offer` | ads | `placement`, `accepted`, `rewarded_ecpm_loaded` |
| `store_offer` | ads | `offer_id`, `price_usd`, `trigger` (store/post_loss) |
| `purchase` | ads | `offer_id`, `price_usd` |

`board` es el tablero como entero de 64 bits en hexadecimal (bit `fila*8+columna`). Se guarda como texto porque Spark no tiene enteros sin signo de 64 bits.

## Verdad terreno

**`deal_labels`**: una fila por reparto.

| Campo | Descripción |
|---|---|
| `deal_id`, `player_id`, `game_id`, `ts`, `policy` | Llaves para unir con `piece_deal` |
| `kind` | `normal`, `lethal` (mano imposible) o `hard` (piezas grandes) |
| `rule` | Regla que lo pidió: `ad_harvest`, `record_margin`, `fast_hand` o null |
| `suppressed_rule` | Regla que el guardián bloqueó (etapa 4) |
| `forced` | `true` si fue una mano letal forzada. **Etiqueta del modelo** |
| `solvable` | Si la mano tenía solución sobre ese tablero (cálculo exhaustivo) |

**`players_truth`**: perfil y rasgos latentes de cada agente (`skill`, `impulsivity`, `chase`, `spend`…). Sirve para analizar a quién afecta la manipulación; nunca entra al modelo.

## Por qué la separación importa

Si el modelo viera `forced` o los rasgos latentes, aprendería la respuesta en lugar de detectarla (fuga de datos). El guardián solo ve lo que vería un auditor externo: tablero, piezas, puntajes, tiempos y anuncios.

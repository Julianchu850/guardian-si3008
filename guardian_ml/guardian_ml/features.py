"""Variables del guardián: lo que un auditor externo puede observar ANTES de
cada reparto de piezas.

Esta es la definición de referencia. Se usa igual en tres lugares:
- el servicio en streaming (consume Kafka evento por evento),
- el guardián embebido en el simulador (decide antes de repartir),
- y el job de Spark spark/jobs/deal_features.py (histórico para entrenar),
  que reproduce esta lógica con ventanas. Una prueba compara ambos resultados.

Ninguna variable usa la verdad terreno ni los rasgos latentes del jugador.
"""
from __future__ import annotations

from dataclasses import dataclass, field

FEATURES = [
    "score",                    # puntaje actual de la partida
    "high_score",               # récord del jugador antes de la partida
    "score_ratio",              # puntaje / récord
    "above_record",             # 1 si ya superó su récord
    "record_margin",            # (puntaje - récord) / récord
    "loaded_ecpm",              # valor del anuncio rewarded cargado ahora
    "last_rewarded_ecpm",       # valor del último rewarded que vio
    "ecpm_ratio",               # cargado / último (la regla depredadora busca >= 3,5)
    "rewarded_last3_completed", # cuántos de sus últimos 3 rewarded vio completos
    "rewarded_count",           # rewarded vistos en total
    "deal_num",                 # número de reparto en la partida
    "game_num",                 # número de partida del jugador
    "revives_in_game",          # revives usados en esta partida
    "avg_turn_s_last6",         # segundos promedio de sus últimas 6 manos
    "board_fill",               # celdas ocupadas del tablero (0-64)
]
LABEL = "forced"


def board_fill(board_hex: str | None) -> int:
    return int(board_hex, 16).bit_count() if board_hex else 0


def compute(score, high_score, loaded_ecpm, last_rewarded_ecpm, rewarded_last3_completed,
            rewarded_count, deal_num, game_num, revives_in_game, avg_turn_s_last6,
            board_fill_value) -> dict:
    score = float(score or 0)
    high = float(high_score or 0)
    loaded = float(loaded_ecpm or 0.0)
    last = float(last_rewarded_ecpm or 0.0)
    return {
        "score": score,
        "high_score": high,
        "score_ratio": score / high if high > 0 else 0.0,
        "above_record": 1.0 if high > 0 and score > high else 0.0,
        "record_margin": (score - high) / high if high > 0 else 0.0,
        "loaded_ecpm": loaded,
        "last_rewarded_ecpm": last,
        "ecpm_ratio": loaded / last if last > 0 else 0.0,
        "rewarded_last3_completed": float(rewarded_last3_completed),
        "rewarded_count": float(rewarded_count),
        "deal_num": float(deal_num or 0),
        "game_num": float(game_num or 0),
        "revives_in_game": float(revives_in_game),
        "avg_turn_s_last6": float(avg_turn_s_last6),
        "board_fill": float(board_fill_value),
    }


@dataclass
class PlayerState:
    """Estado observable de un jugador, actualizado evento por evento."""
    rewarded: list[bool] = field(default_factory=list)   # el más reciente primero
    last_rewarded_ecpm: float = 0.0
    rewarded_count: int = 0
    game_id: str | None = None
    game_num: int = 0
    revives_in_game: int = 0
    turn_ms: list[int] = field(default_factory=list)

    def update(self, e: dict) -> None:
        et = e.get("event_type")
        if et == "game_start":
            self.game_id = e.get("game_id")
            self.game_num = int(e.get("game_num") or 0)
            self.revives_in_game = 0
            self.turn_ms = []
        elif et == "turn_result":
            self.turn_ms.append(int(e.get("turn_ms") or 0))
        elif et == "revive":
            self.revives_in_game += 1
        elif et == "ad_impression" and e.get("ad_type") == "rewarded":
            self.rewarded.insert(0, bool(e.get("completed")))
            del self.rewarded[3:]
            self.last_rewarded_ecpm = float(e.get("ecpm") or 0.0)
            self.rewarded_count += 1

    def features(self, score, high_score, loaded_ecpm, deal_num, board_hex) -> dict:
        last6 = self.turn_ms[-6:]
        return compute(score, high_score, loaded_ecpm, self.last_rewarded_ecpm,
                       sum(self.rewarded[:3]), self.rewarded_count, deal_num, self.game_num,
                       self.revives_in_game, (sum(last6) / len(last6) / 1000.0) if last6 else 0.0,
                       board_fill(board_hex))

    def features_for_deal(self, deal_event: dict) -> dict:
        return self.features(deal_event.get("score"), deal_event.get("high_score"),
                             deal_event.get("rewarded_ecpm_loaded"), deal_event.get("deal_num"),
                             deal_event.get("board"))


def event_seq(event_id: str) -> int:
    """Secuencia por jugador: el número final de event_id (p42-00017-1234 → 1234)."""
    return int(event_id.rsplit("-", 1)[1])


def features_from_events(events) -> list[dict]:
    """Reproduce el cálculo de streaming sobre una lista de eventos (para pruebas
    y para comparar con Spark). Devuelve una fila por piece_deal."""
    states: dict[tuple, PlayerState] = {}
    rows = []
    for e in sorted(events, key=lambda x: (x["player_id"], x["policy"], event_seq(x["event_id"]))):
        st = states.setdefault((e["player_id"], e["policy"]), PlayerState())
        if e["event_type"] == "piece_deal":
            rows.append({"deal_id": e["deal_id"], "player_id": e["player_id"],
                         "policy": e["policy"], **st.features_for_deal(e)})
        st.update(e)
    return rows

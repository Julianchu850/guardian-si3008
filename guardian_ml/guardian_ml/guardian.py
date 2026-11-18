"""El guardián: estado por jugador + modelo + política de intervención.

Intervenciones:
- block_manipulation: el riesgo de que el próximo reparto sea manipulado supera
  el umbral del modelo. El juego no aplica reglas depredadoras al jugador
  durante el resto de la partida.
- suggest_break: en una misma sesión el jugador perdió 2 veces justo después
  de superar su récord (patrón de frustración que alimenta el juego compulsivo).
- spend_limit: 2 compras en una misma sesión; se bloquean compras el resto de ella.

La misma clase se usa en dos modos:
- embebido (simulador / backend del juego): pre_deal() decide ANTES de repartir;
- streaming (servicio sobre Kafka): observe() puntúa cada reparto que llega y
  emite intervenciones para los siguientes.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .features import PlayerState
from .model import RiskModel

LOSSES_AFTER_RECORD_FOR_BREAK = 2
PURCHASES_FOR_LIMIT = 2


@dataclass
class GuardState:
    features: PlayerState = field(default_factory=PlayerState)
    session_id: str | None = None
    losses_after_record: int = 0
    purchases: int = 0
    session_actions: set[str] = field(default_factory=set)
    blocked_game: str | None = None


class Guardian:
    def __init__(self, model: RiskModel, threshold: float | None = None):
        self.model = model
        self.threshold = model.threshold if threshold is None else threshold
        self.states: dict[tuple[str, str], GuardState] = {}

    def state(self, player_id: str, policy: str) -> GuardState:
        return self.states.setdefault((player_id, policy), GuardState())

    # ------------------------------------------------------------------
    def observe(self, e: dict, st: GuardState | None = None, score_deals: bool = True) -> list[dict]:
        """Procesa un evento (en orden por jugador). Devuelve intervenciones nuevas.

        score_deals=False en modo embebido: allí el riesgo ya se calculó en pre_deal()."""
        st = st or self.state(e["player_id"], e["policy"])
        out: list[dict] = []
        et = e["event_type"]
        if et == "session_start":
            st.session_id = e.get("session_id")
            st.losses_after_record = st.purchases = 0
            st.session_actions = set()
        elif et == "piece_deal" and score_deals:
            risk = self.model.risk(st.features.features_for_deal(e))
            if risk >= self.threshold and st.blocked_game != e.get("game_id"):
                st.blocked_game = e.get("game_id")
                out.append(self._iv(e, "block_manipulation", risk, "riesgo de reparto manipulado"))
        elif et == "game_over" and e.get("above_record"):
            st.losses_after_record += 1
            if (st.losses_after_record >= LOSSES_AFTER_RECORD_FOR_BREAK
                    and "suggest_break" not in st.session_actions):
                st.session_actions.add("suggest_break")
                out.append(self._iv(e, "suggest_break", None,
                                    f"{st.losses_after_record} derrotas tras superar el récord"))
        elif et == "purchase":
            st.purchases += 1
            if st.purchases >= PURCHASES_FOR_LIMIT and "spend_limit" not in st.session_actions:
                st.session_actions.add("spend_limit")
                out.append(self._iv(e, "spend_limit", None, f"{st.purchases} compras en la sesión"))
        st.features.update(e)
        return out

    def pre_deal(self, st: GuardState, game_id: str, score, high_score, loaded_ecpm,
                 deal_num, board_hex) -> tuple[bool, float]:
        """Modo embebido: ¿bloquear la manipulación en el reparto que viene?"""
        if st.blocked_game == game_id:
            return True, 1.0
        risk = self.model.risk(st.features.features(score, high_score, loaded_ecpm,
                                                    deal_num, board_hex))
        if risk >= self.threshold:
            st.blocked_game = game_id
            return True, risk
        return False, risk

    def _iv(self, e: dict, action: str, risk: float | None, reason: str) -> dict:
        return {"intervention_id": f"{e['event_id']}-{action}", "ts": e["ts"],
                "player_id": e["player_id"], "policy": e["policy"],
                "session_id": e.get("session_id"), "game_id": e.get("game_id"),
                "trigger_event_id": e["event_id"], "action": action,
                "risk": None if risk is None else round(risk, 5),
                "threshold": round(self.threshold, 5), "reason": reason,
                "model_version": self.model.version}

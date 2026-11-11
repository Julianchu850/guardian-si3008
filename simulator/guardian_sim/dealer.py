"""Repartidor de piezas y políticas de monetización.

- Política "neutral": reparte piezas al azar según su frecuencia.
- Política "predatory": añade reglas abstraídas del análisis de Block Blast
  (Omolewa, 2026). No replica su código; reproduce sus condiciones:

  * ad_harvest: con los últimos 3 anuncios rewarded completados, un anuncio
    cargado que vale >= 3,5x el anterior y el puntaje por encima del récord
    + delta, pide una mano letal (una vez por partida; tras revivir, el
    umbral sube otro delta y puede volver a disparar).
  * record_margin: superado el récord en un 10 %, cada reparto tiene un 20 %
    de probabilidad de ser letal.
  * fast_hand: si los últimos 6 repartos se jugaron en < 4,2 s de media y el
    puntaje pasa el 93 % del récord, reparte piezas grandes.

Una "mano letal" son 3 piezas donde 2 caben ahora, 1 no cabe, y ninguna
combinación de orden y posiciones permite colocarlas todas.

El guardián (etapa 4) puede bloquear estas reglas para un jugador; en ese
caso la regla queda registrada como "suprimida" en la tabla de etiquetas.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from . import engine

POLICIES = ("neutral", "predatory")

AD_HIGH_MULTIPLIER = 3.5
AD_MIN_HISTORY = 3
AD_DELTAS = (500, 750, 1000)      # "arms" del umbral sobre el récord
RECORD_MARGIN_PCT = 0.10
RECORD_MARGIN_PROB = 0.20
FAST_HAND_SECONDS = 4.2
FAST_HAND_RECORD_PCT = 0.93

_WEIGHTS = np.array([p.weight for p in engine.PIECES])
_WEIGHTS = _WEIGHTS / _WEIGHTS.sum()


@dataclass
class GameRuleState:
    """Estado de las reglas durante una partida."""
    ad_delta: int
    ad_bar: int
    ad_fired: bool = False
    revive_round: bool = False


@dataclass
class DealContext:
    board: int
    score: int
    high_score: int
    game_num: int                 # partidas jugadas por el jugador (0 = primera)
    rewarded_history: list[bool]  # resultados de rewarded, el más reciente primero
    last_rewarded_ecpm: float     # eCPM del último rewarded mostrado (0 si ninguno)
    loaded_rewarded_ecpm: float   # eCPM del rewarded cargado ahora
    recent_turn_secs: list[float] = field(default_factory=list)
    deal_num: int = 0             # repartos ya hechos en esta partida


@dataclass
class Deal:
    pieces: list[int]
    kind: str                     # normal | lethal | hard
    rule: str | None              # regla que la pidió (None si normal)
    suppressed_rule: str | None = None


_LARGE = np.array([p.size >= 5 for p in engine.PIECES])


FAIR_REROLLS = 10


def random_hand(rng: np.random.Generator, deal_num: int = 0, board: int | None = None) -> list[int]:
    """Mano justa: al azar, pero con solución sobre el tablero actual.

    Si la mano sorteada no tiene solución se vuelve a sortear (hasta
    FAIR_REROLLS veces), así las derrotas vienen de los errores del jugador y
    no del azar. La dificultad sube igual para todos a medida que avanza la
    partida (más piezas grandes); no depende del jugador ni de los anuncios.
    """
    ramp = min(deal_num / 60.0, 1.0)
    w = _WEIGHTS * np.where(_LARGE, 1.0 + 2.0 * ramp, 1.0)
    w = w / w.sum()
    hand = [int(x) for x in rng.choice(engine.N_PIECES, size=3, p=w)]
    if board is None:
        return hand
    for _ in range(FAIR_REROLLS):
        if engine.solvable(board, hand):
            break
        hand = [int(x) for x in rng.choice(engine.N_PIECES, size=3, p=w)]
    return hand


def lethal_hand(board: int, rng: np.random.Generator, attempts: int = 120) -> list[int] | None:
    """Busca 2 piezas que caben y 1 que no, sin solución en ningún orden."""
    fit = [p.id for p in engine.PIECES if engine.fits(board, p.id)]
    fit_set = set(fit)
    no_fit = [p.id for p in engine.PIECES if p.id not in fit_set]
    if len(fit) < 2 or not no_fit:
        return None
    for _ in range(attempts):
        a, b = rng.choice(fit, size=2, replace=True)
        c = rng.choice(no_fit)
        hand = [int(a), int(b), int(c)]
        if not engine.solvable(board, hand):
            rng.shuffle(hand)
            return hand
    return None


def hard_hand(rng: np.random.Generator) -> list[int]:
    return [int(x) for x in rng.choice(engine.LARGE_PIECES, size=3)]


class Dealer:
    def __init__(self, policy: str):
        if policy not in POLICIES:
            raise ValueError(f"política desconocida: {policy}")
        self.policy = policy

    def new_game_state(self, high_score: int, rng: np.random.Generator) -> GameRuleState:
        delta = int(rng.choice(AD_DELTAS))
        return GameRuleState(ad_delta=delta, ad_bar=high_score + delta)

    def on_revive(self, state: GameRuleState) -> None:
        state.revive_round = True
        if state.ad_fired:
            state.ad_bar += state.ad_delta
            state.ad_fired = False

    def deal(self, ctx: DealContext, state: GameRuleState, rng: np.random.Generator,
             blocked: bool = False) -> Deal:
        if self.policy == "neutral":
            return Deal(random_hand(rng, ctx.deal_num, ctx.board), "normal", None)

        rule, kind = self._which_rule(ctx, state, rng)
        if rule is None:
            return Deal(random_hand(rng, ctx.deal_num, ctx.board), "normal", None)
        if blocked:
            return Deal(random_hand(rng, ctx.deal_num, ctx.board), "normal", None, suppressed_rule=rule)
        if kind == "hard":
            return Deal(hard_hand(rng), "hard", rule)
        hand = lethal_hand(ctx.board, rng)
        if hand is None:  # el generador no encontró mano: reintenta en el próximo reparto
            return Deal(random_hand(rng, ctx.deal_num, ctx.board), "normal", None)
        if rule == "ad_harvest":
            state.ad_fired = True
        return Deal(hand, "lethal", rule)

    # ------------------------------------------------------------------
    def _ad_harvest(self, ctx: DealContext, state: GameRuleState) -> bool:
        hist = ctx.rewarded_history
        if len(hist) < AD_MIN_HISTORY or not all(hist[:AD_MIN_HISTORY]):
            return False
        if state.ad_fired or ctx.score < state.ad_bar or ctx.last_rewarded_ecpm <= 0:
            return False
        return ctx.loaded_rewarded_ecpm >= ctx.last_rewarded_ecpm * AD_HIGH_MULTIPLIER

    def _which_rule(self, ctx: DealContext, state: GameRuleState,
                    rng: np.random.Generator) -> tuple[str | None, str | None]:
        if self._ad_harvest(ctx, state):
            return "ad_harvest", "lethal"
        if (ctx.game_num > 0 and not state.revive_round and ctx.high_score > 0
                and ctx.score > ctx.high_score * (1 + RECORD_MARGIN_PCT)
                and rng.random() < RECORD_MARGIN_PROB):
            return "record_margin", "lethal"
        recent = ctx.recent_turn_secs[-6:]
        if (len(recent) == 6 and sum(recent) / 6 < FAST_HAND_SECONDS
                and ctx.high_score > 0 and ctx.score > ctx.high_score * FAST_HAND_RECORD_PCT):
            return "fast_hand", "hard"
        return None, None

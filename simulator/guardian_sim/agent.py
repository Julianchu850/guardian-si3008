"""Cerebro de los agentes-jugador.

Todos los agentes usan la misma regla de decisión: enumerar las jugadas
posibles, puntuar el tablero que resulta de cada una y elegir con una
softmax. El perfil cambia los parámetros:

- habilidad: baja la temperatura (menos errores), revisa más jugadas y mira
  si las piezas restantes seguirán cabiendo (planear la mano completa);
- estilo: pesos de combos frente a orden del tablero;
- velocidad y frustración: la frustración ("tilt") sube la temperatura y
  acelera las jugadas, como una persona molesta.

Las decisiones fuera del tablero (revivir, ver el anuncio, comprar, seguir
jugando, volver mañana) son probabilidades logísticas que dependen del perfil
y del contexto. Ahí es donde actúa la manipulación.
"""
from __future__ import annotations

import math

import numpy as np

from . import engine
from .profiles import Traits


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


class Brain:
    def __init__(self, traits: Traits):
        self.t = traits
        # Pesos de la función de evaluación según el estilo.
        self.w_lines = 3.0 + 2.0 * traits.combo_lover
        self.w_combo = 4.0 * traits.combo_lover
        self.w_holes = 2.5 * (1.2 - traits.combo_lover)
        self.w_space = 1.0 + 1.5 * traits.skill
        self.w_frag = 0.15
        self.w_feas = 6.0 * traits.skill            # planear la mano completa
        self.max_candidates = int(15 + traits.skill * 185)

    # ------------------------------------------------------------------
    # Jugar en el tablero
    # ------------------------------------------------------------------
    def temperature(self, frustration: float) -> float:
        return 0.15 + (1.0 - self.t.skill) * 1.6 + 0.35 * min(frustration, 4.0)

    def choose_move(self, board: int, hand: list[int], rng: np.random.Generator,
                    frustration: float) -> tuple[int, int] | None:
        """Devuelve (índice de la pieza en la mano, máscara) o None si nada cabe."""
        candidates: list[tuple[int, int]] = []
        seen: set[int] = set()
        for i, pid in enumerate(hand):
            if pid in seen:
                continue
            seen.add(pid)
            candidates.extend((i, m) for m in engine.fitting_placements(board, pid))
        if not candidates:
            return None
        if len(candidates) > self.max_candidates:
            idx = rng.choice(len(candidates), size=self.max_candidates, replace=False)
            candidates = [candidates[k] for k in idx]

        scores = np.empty(len(candidates))
        for k, (i, m) in enumerate(candidates):
            nb, lines = engine.place(board, m)
            rest = hand[:i] + hand[i + 1:]
            feas = sum(engine.fits(nb, p) for p in rest) - len(rest)  # 0 si todas caben
            scores[k] = (self.w_lines * lines
                         + self.w_combo * (lines >= 2)
                         - self.w_holes * engine.holes(nb)
                         + self.w_space * min(engine.free_3x3(nb), 4) / 4
                         - self.w_frag * engine.fragmentation(nb)
                         + self.w_feas * feas)
        z = scores / self.temperature(frustration)
        z -= z.max()
        p = np.exp(z)
        p /= p.sum()
        return candidates[rng.choice(len(candidates), p=p)]

    def move_seconds(self, rng: np.random.Generator, frustration: float) -> float:
        tilt = 1.0 - 0.08 * min(frustration, 4.0) * self.t.chase
        return float(rng.lognormal(math.log(self.t.speed_s), 0.35) * tilt)

    # ------------------------------------------------------------------
    # Decisiones fuera del tablero
    # ------------------------------------------------------------------
    def accept_revive(self, score: int, high_score: int, revives_used: int,
                      frustration: float, rng: np.random.Generator) -> bool:
        ratio = score / high_score if high_score > 0 else 0.0
        logit = (-1.8 + 3.2 * self.t.impulsivity
                 + 1.2 * (ratio >= 1.0) + 0.6 * (ratio >= 0.8)
                 + 0.5 * self.t.chase * min(frustration, 4.0)
                 - 0.9 * revives_used)
        return rng.random() < sigmoid(logit)

    def watch_ad(self, ad_type: str, rng: np.random.Generator) -> tuple[bool, float]:
        """¿Ve el anuncio completo? Devuelve (completado, segundos vistos)."""
        if ad_type == "rewarded":
            complete = rng.random() < min(0.98, 0.45 + 0.55 * self.t.patience)
            secs = rng.uniform(15, 30) if complete else rng.uniform(2, 10)
        else:
            complete = rng.random() < 0.15 + 0.5 * self.t.patience
            secs = rng.uniform(15, 30) if complete else rng.uniform(5, 7)
        return bool(complete), float(secs)

    def buy(self, price: float, spent: float, frustration: float,
            rng: np.random.Generator) -> bool:
        if spent + price > self.t.budget_usd:
            return False
        drive = 0.3 + 0.4 * min(frustration, 3.0) / 3.0 + 0.3 * self.t.chase
        return rng.random() < self.t.spend * drive * 0.5

    def continue_session(self, games_in_session: int, frustration: float,
                         lost_near_record: bool, new_record: bool,
                         rng: np.random.Generator) -> bool:
        logit = (0.6 + 2.2 * self.t.tolerance
                 - 0.28 * games_in_session * (1.0 - 0.6 * self.t.chase)
                 + 1.4 * self.t.chase * lost_near_record
                 + 0.4 * new_record
                 - 0.45 * frustration * (1.0 - self.t.chase))
        return rng.random() < sigmoid(logit)

    def return_tomorrow(self, unfair_losses_today: int, rng: np.random.Generator) -> bool:
        p = self.t.retention * math.exp(-0.12 * unfair_losses_today * (1.0 - self.t.chase))
        p += 0.05 * self.t.chase * min(unfair_losses_today, 3)
        return rng.random() < min(p, 0.97)

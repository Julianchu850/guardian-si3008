"""Perfiles de jugador y muestreo de rasgos latentes.

Cada perfil define la media y la desviación de sus rasgos. Cada agente toma
valores propios alrededor de esas medias, así dos "casuales" no juegan igual.
Los rasgos son latentes: nunca aparecen en los eventos, solo en la tabla de
verdad terreno (players_truth), y el modelo guardián no puede usarlos.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np

# Mezcla de la población simulada.
PROFILE_MIX = {
    "casual": 0.45,
    "competitive": 0.20,
    "spender": 0.17,
    "whale": 0.05,
    "loss_chaser": 0.13,
}

# rasgo: (media, desviación). Rasgos en [0, 1] salvo indicación.
_PROFILE_PARAMS: dict[str, dict[str, tuple[float, float]]] = {
    "casual": dict(
        skill=(0.35, 0.10), speed_s=(3.2, 0.6), patience=(0.55, 0.15),
        impulsivity=(0.25, 0.10), tolerance=(0.35, 0.10), chase=(0.10, 0.05),
        spend=(0.02, 0.02), budget_usd=(2.0, 1.5), sessions_per_day=(1.5, 0.5),
        retention=(0.55, 0.10), night_owl=(0.10, 0.05), combo_lover=(0.30, 0.15)),
    "competitive": dict(
        skill=(0.80, 0.08), speed_s=(1.8, 0.4), patience=(0.60, 0.15),
        impulsivity=(0.45, 0.10), tolerance=(0.70, 0.10), chase=(0.30, 0.10),
        spend=(0.05, 0.03), budget_usd=(5.0, 3.0), sessions_per_day=(3.0, 0.8),
        retention=(0.80, 0.08), night_owl=(0.35, 0.10), combo_lover=(0.70, 0.15)),
    "spender": dict(
        skill=(0.50, 0.12), speed_s=(2.5, 0.5), patience=(0.75, 0.10),
        impulsivity=(0.70, 0.10), tolerance=(0.55, 0.10), chase=(0.35, 0.10),
        spend=(0.35, 0.10), budget_usd=(25.0, 10.0), sessions_per_day=(2.5, 0.7),
        retention=(0.70, 0.08), night_owl=(0.25, 0.10), combo_lover=(0.50, 0.15)),
    "whale": dict(
        skill=(0.55, 0.12), speed_s=(2.3, 0.5), patience=(0.85, 0.08),
        impulsivity=(0.80, 0.08), tolerance=(0.70, 0.10), chase=(0.45, 0.10),
        spend=(0.60, 0.10), budget_usd=(150.0, 50.0), sessions_per_day=(3.5, 0.8),
        retention=(0.85, 0.06), night_owl=(0.40, 0.10), combo_lover=(0.50, 0.15)),
    "loss_chaser": dict(
        skill=(0.45, 0.12), speed_s=(2.0, 0.4), patience=(0.70, 0.10),
        impulsivity=(0.85, 0.07), tolerance=(0.90, 0.05), chase=(0.90, 0.05),
        spend=(0.30, 0.10), budget_usd=(30.0, 12.0), sessions_per_day=(3.0, 0.8),
        retention=(0.80, 0.08), night_owl=(0.60, 0.10), combo_lover=(0.60, 0.15)),
}

_UNBOUNDED = {"speed_s": (0.8, 6.0), "budget_usd": (0.0, 500.0),
              "sessions_per_day": (0.5, 8.0)}


@dataclass
class Traits:
    skill: float
    speed_s: float            # segundos medios por colocación
    patience: float           # prob. base de ver un anuncio completo
    impulsivity: float        # propensión a aceptar revives
    tolerance: float          # aguante a las derrotas antes de irse
    chase: float              # persecución de pérdidas
    spend: float              # propensión a comprar
    budget_usd: float         # presupuesto mensual
    sessions_per_day: float
    retention: float          # prob. base de volver al día siguiente
    night_owl: float          # prob. de que una sesión sea nocturna
    combo_lover: float        # estilo: prefiere combos arriesgados

    def as_dict(self) -> dict:
        return {k: round(v, 4) for k, v in asdict(self).items()}


def sample_profile(rng: np.random.Generator) -> str:
    names = list(PROFILE_MIX)
    p = np.array([PROFILE_MIX[n] for n in names])
    return names[rng.choice(len(names), p=p / p.sum())]


def sample_traits(profile: str, rng: np.random.Generator) -> Traits:
    values = {}
    for trait, (mean, sd) in _PROFILE_PARAMS[profile].items():
        lo, hi = _UNBOUNDED.get(trait, (0.0, 1.0))
        values[trait] = float(np.clip(rng.normal(mean, sd), lo, hi))
    return Traits(**values)

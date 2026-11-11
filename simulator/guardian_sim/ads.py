"""Mercado de anuncios simulado.

Cada jugador tiene un valor base de anuncio (eCPM, dólares por mil
impresiones) que depende de su país, plataforma y gama de dispositivo, como
el modelo de valor del jugador descrito en el caso de referencia. El anuncio
cargado en memoria cambia de precio con el tiempo; esa variación es la que
aprovecha la regla depredadora (esperar un anuncio >= 3,5 veces más caro).

Los valores son supuestos de calibración, no datos reales: se documentan
como tales en el informe.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

COUNTRY_SHARE = {"CO": 0.20, "US": 0.18, "BR": 0.16, "MX": 0.14, "IN": 0.12,
                 "PH": 0.10, "DE": 0.05, "GB": 0.05}
COUNTRY_ECPM = {"US": 24.0, "DE": 18.0, "GB": 17.0, "MX": 8.0, "BR": 7.5,
                "CO": 6.5, "PH": 4.5, "IN": 4.0}
PLATFORM_SHARE = {"android": 0.7, "ios": 0.3}
PLATFORM_FACTOR = {"android": 1.0, "ios": 1.15}
DEVICE_SHARE = {"low": 0.35, "mid": 0.45, "high": 0.20}
DEVICE_FACTOR = {"low": 0.7, "mid": 1.0, "high": 1.3}

REWARDED_SIGMA = 0.65       # dispersión del precio de cada anuncio cargado
INTERSTITIAL_RATIO = 0.6    # un intersticial paga menos que un rewarded
REFRESH_PROB = 0.25         # prob. de que se recargue el anuncio en cada reparto


def base_ecpm(country: str, platform: str, device_tier: str) -> float:
    return COUNTRY_ECPM[country] * PLATFORM_FACTOR[platform] * DEVICE_FACTOR[device_tier]


@dataclass
class AdSlots:
    """Anuncios cargados en el teléfono de un jugador en este momento."""
    base: float
    rewarded_ecpm: float = 0.0
    interstitial_ecpm: float = 0.0

    def load_all(self, rng: np.random.Generator) -> None:
        self.load_rewarded(rng)
        self.load_interstitial(rng)

    def load_rewarded(self, rng: np.random.Generator) -> None:
        self.rewarded_ecpm = round(self.base * rng.lognormal(0.0, REWARDED_SIGMA), 4)

    def load_interstitial(self, rng: np.random.Generator) -> None:
        self.interstitial_ecpm = round(
            self.base * INTERSTITIAL_RATIO * rng.lognormal(0.0, 0.5), 4)

    def maybe_refresh(self, rng: np.random.Generator) -> None:
        if rng.random() < REFRESH_PROB:
            self.load_rewarded(rng)


def sample_choice(rng: np.random.Generator, shares: dict[str, float]) -> str:
    keys = list(shares)
    p = np.array([shares[k] for k in keys])
    return keys[rng.choice(len(keys), p=p / p.sum())]

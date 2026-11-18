"""Consultas del dashboard y las métricas que se calculan con sus resultados.

El SQL corre igual en Athena (Trino) y en DuckDB. Las divisiones se hacen en
pandas: en Trino, entero / entero es división entera.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

import pandas as pd

WORLDS = ["neutral", "predatory", "guarded"]
WORLD_LABEL = {"neutral": "Neutral", "predatory": "Depredador", "guarded": "Con guardián"}
RULE_LABEL = {"ad_harvest": "Anuncio caro", "record_margin": "Margen sobre récord",
              "fast_hand": "Jugador rápido", "none": "Sin regla"}
PROFILE_LABEL = {"casual": "Casual", "competitive": "Competitivo", "spender": "Gastador",
                 "whale": "Ballena", "loss_chaser": "Persigue pérdidas"}
ACTION_LABEL = {"block_manipulation": "Bloquear manipulación", "suggest_break": "Sugerir pausa",
                "spend_limit": "Límite de gasto"}
_PREFIX = re.compile(r"^p\d+$")


@dataclass(frozen=True)
class Filters:
    d0: date
    d1: date
    population: str | None = None          # p.ej. "p42"; None = todas
    worlds: tuple[str, ...] = tuple(WORLDS)

    def where(self, alias: str = "") -> str:
        a = f"{alias}." if alias else ""
        parts = [f"{a}dt BETWEEN DATE '{self.d0.isoformat()}' AND DATE '{self.d1.isoformat()}'"]
        if self.population:
            if not _PREFIX.match(self.population):
                raise ValueError("población inválida")
            parts.append(f"{a}player_id LIKE '{self.population}-%'")
        worlds = [w for w in self.worlds if w in WORLDS] or WORLDS
        parts.append(f"{a}policy IN ({', '.join(repr(w) for w in worlds)})")
        return " AND ".join(parts)


# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------
def sql_populations() -> str:
    return """
SELECT split_part(player_id, '-', 1) AS poblacion,
       count(DISTINCT player_id) AS jugadores,
       count(DISTINCT policy) AS mundos,
       min(dt) AS desde, max(dt) AS hasta
FROM guardian.gold_player_daily
GROUP BY 1 ORDER BY 2 DESC"""


SUMS = """count(DISTINCT player_id) AS jugadores, sum(games) AS partidas,
       sum(play_seconds) AS segundos, sum(sessions) AS sesiones,
       sum(night_sessions) AS sesiones_noche, sum(ad_impressions) AS anuncios,
       sum(ad_revenue_usd) AS ingreso_anuncios, sum(purchases_usd) AS compras,
       sum(game_overs) AS derrotas, sum(game_overs_above_record) AS derrotas_tras_record,
       sum(revive_offers) AS ofertas_revive, sum(revives_accepted) AS revives_aceptados"""


def sql_worlds(f: Filters) -> str:
    return f"SELECT policy, {SUMS}\nFROM guardian.gold_player_daily WHERE {f.where()}\nGROUP BY policy"


def sql_daily(f: Filters) -> str:
    return (f"SELECT dt, policy, {SUMS}\nFROM guardian.gold_player_daily WHERE {f.where()}\n"
            "GROUP BY dt, policy ORDER BY dt")


def sql_player_revenue(f: Filters) -> str:
    return f"""
SELECT policy, player_id, sum(total_revenue_usd) AS ingreso
FROM guardian.gold_player_daily WHERE {f.where()}
GROUP BY policy, player_id"""


def sql_forced_by_rule(f: Filters) -> str:
    return f"""
SELECT policy, coalesce(rule, suppressed_rule, 'none') AS regla,
       count(*) AS repartos,
       sum(CASE WHEN forced THEN 1 ELSE 0 END) AS forzados,
       sum(CASE WHEN suppressed_rule IS NOT NULL THEN 1 ELSE 0 END) AS bloqueados
FROM guardian.silver_deal_labels WHERE {f.where()}
GROUP BY 1, 2"""


def sql_by_profile(f: Filters) -> str:
    return f"""
SELECT p.policy, t.profile AS perfil, count(DISTINCT p.player_id) AS jugadores,
       sum(p.games) AS partidas, sum(p.total_revenue_usd) AS ingreso,
       sum(p.game_overs_above_record) AS derrotas_tras_record
FROM guardian.gold_player_daily p
JOIN guardian.silver_players_truth t ON p.player_id = t.player_id AND p.policy = t.policy
WHERE {f.where('p')}
GROUP BY 1, 2"""


def sql_forced_by_profile(f: Filters) -> str:
    return f"""
SELECT l.policy, t.profile AS perfil,
       sum(CASE WHEN l.forced THEN 1 ELSE 0 END) AS forzados
FROM guardian.silver_deal_labels l
JOIN guardian.silver_players_truth t ON l.player_id = t.player_id AND l.policy = t.policy
WHERE {f.where('l')}
GROUP BY 1, 2"""


def sql_interventions_embedded(f: Filters) -> str:
    return f"""
SELECT policy, action AS accion, count(*) AS intervenciones,
       count(DISTINCT player_id) AS jugadores
FROM guardian.silver_game_events
WHERE event_type = 'intervention_applied' AND {f.where()}
GROUP BY 1, 2"""


def sql_interventions_stream() -> str:
    return """
SELECT action AS accion, count(*) AS intervenciones, count(DISTINCT player_id) AS jugadores,
       avg(risk) AS riesgo_medio
FROM guardian.bronze_interventions GROUP BY 1"""


def sql_interventions_recent(limit: int = 15) -> str:
    return f"""
SELECT ts, player_id, policy, action AS accion, risk AS riesgo, reason AS motivo
FROM guardian.bronze_interventions ORDER BY ts DESC LIMIT {int(limit)}"""


def sql_freshness() -> str:
    parts = []
    for t in ("gold_player_daily", "gold_harm_daily", "gold_deal_features",
              "silver_game_events", "silver_ad_events", "silver_deal_labels"):
        parts.append(f"SELECT '{t}' AS tabla, count(*) AS filas, min(dt) AS desde, max(dt) AS hasta "
                     f"FROM guardian.{t}")
    return "\nUNION ALL\n".join(parts)


# --------------------------------------------------------------------------
# Métricas derivadas
# --------------------------------------------------------------------------
def _div(a, b):
    return a / b.where(b != 0)


def world_metrics(worlds: pd.DataFrame, forced: pd.DataFrame, player_rev: pd.DataFrame) -> pd.DataFrame:
    w = worlds.set_index("policy").astype(float)
    out = pd.DataFrame(index=w.index)
    out["jugadores"] = w.jugadores
    out["ingreso_por_jugador"] = _div(w.ingreso_anuncios + w.compras, w.jugadores)
    out["anuncios_por_jugador"] = _div(w.ingreso_anuncios, w.jugadores)
    out["anuncios_hora"] = _div(w.anuncios, w.segundos / 3600)
    out["derrotas_tras_record"] = _div(w.derrotas_tras_record, w.derrotas)
    out["acepta_revive"] = _div(w.revives_aceptados, w.ofertas_revive)
    out["sesiones_noche"] = _div(w.sesiones_noche, w.sesiones)
    f = forced.groupby("policy")[["forzados", "bloqueados"]].sum() if len(forced) else \
        pd.DataFrame(columns=["forzados", "bloqueados"])
    out["forzados"] = f["forzados"].reindex(out.index).fillna(0)
    out["bloqueados"] = f["bloqueados"].reindex(out.index).fillna(0)
    out["letales_100_partidas"] = 100 * _div(out.forzados, w.partidas)
    top = {}
    for pol, g in player_rev.groupby("policy"):
        s = g.ingreso.sort_values(ascending=False)
        k = max(1, round(len(s) * 0.05))
        top[pol] = s.head(k).sum() / s.sum() if s.sum() else float("nan")
    out["ingreso_top5"] = pd.Series(top).reindex(out.index)
    order = [x for x in WORLDS if x in out.index]
    return out.loc[order]


def daily_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    d = daily.copy()
    d["ingreso_por_jugador"] = _div(d.ingreso_anuncios.astype(float) + d.compras, d.jugadores.astype(float))
    d["anuncios_hora"] = _div(d.anuncios.astype(float), d.segundos / 3600)
    d["mundo"] = d.policy.map(WORLD_LABEL)
    d["fecha"] = pd.to_datetime(d.dt)
    return d


def forced_by_rule_metrics(forced: pd.DataFrame, worlds: pd.DataFrame) -> pd.DataFrame:
    games = worlds.set_index("policy").partidas.astype(float)
    f = forced[forced.regla != "none"].copy()
    f["letales_100_partidas"] = 100 * f.forzados / f.policy.map(games)
    f["mundo"] = f.policy.map(WORLD_LABEL)
    f["regla_txt"] = f.regla.map(RULE_LABEL).fillna(f.regla)
    return f


def profile_metrics(by_profile: pd.DataFrame, forced_profile: pd.DataFrame) -> pd.DataFrame:
    p = by_profile.merge(forced_profile, on=["policy", "perfil"], how="left").fillna({"forzados": 0})
    p["letales_100_partidas"] = 100 * p.forzados / p.partidas.where(p.partidas != 0)
    p["ingreso_por_jugador"] = p.ingreso / p.jugadores
    tot = p.groupby("policy").ingreso.transform("sum")
    p["participacion_ingreso"] = p.ingreso / tot.where(tot != 0)
    p["mundo"] = p.policy.map(WORLD_LABEL)
    p["perfil_txt"] = p.perfil.map(PROFILE_LABEL).fillna(p.perfil)
    return p

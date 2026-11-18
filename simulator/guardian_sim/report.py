"""Reporte rápido de validación de una corrida del simulador.

Sirve para dos cosas antes de montar el pipeline completo:
1. Comprobar que los perfiles producen comportamientos distintos.
2. Ver una primera versión de las métricas de "dos mundos".

Uso: python -m guardian_sim.report ../data
     python -m guardian_sim.report ../data/neutral ../data/predatory  (dos mundos)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def _read(root: Path, pattern: str) -> pd.DataFrame:
    files = sorted(root.glob(pattern))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_json(f, lines=True) for f in files), ignore_index=True)


def load(root: str | Path) -> dict[str, pd.DataFrame]:
    root = Path(root)
    return {
        "game": _read(root, "bronze/game-events/dt=*/hr=*/*.jsonl"),
        "ads": _read(root, "bronze/ad-events/dt=*/hr=*/*.jsonl"),
        "labels": _read(root, "truth/deal_labels/dt=*/*.jsonl"),
        "truth": _read(root, "truth/players_truth/*.jsonl"),
    }


def top_share(s: pd.Series, q: float = 0.05) -> float:
    s = s.sort_values(ascending=False)
    total = s.sum()
    if total == 0:
        return 0.0
    return float(s.head(max(1, int(round(len(s) * q)))).sum() / total)


def metrics(d: dict[str, pd.DataFrame], by: list[str]) -> pd.DataFrame:
    g, a, lab, truth = d["game"], d["ads"], d["labels"], d["truth"]
    who = truth[["player_id", "profile", "policy"]]

    games = g[g.event_type == "game_end"].merge(who, on=["player_id", "policy"])
    overs = g[g.event_type == "game_over"].merge(who, on=["player_id", "policy"])
    offers = g[g.event_type == "revive_response"].merge(who, on=["player_id", "policy"])
    sess = g[g.event_type == "session_end"].merge(who, on=["player_id", "policy"])
    starts = g[g.event_type == "session_start"].merge(who, on=["player_id", "policy"])
    ads = a.merge(who, on=["player_id", "policy"]) if len(a) else a
    imp = ads[ads.event_type == "ad_impression"] if len(ads) else ads
    buys = ads[ads.event_type == "purchase"] if len(ads) else ads
    lab = lab.merge(who, on=["player_id", "policy"])

    # Un mismo player_id existe en los dos mundos: la clave es (player_id, policy).
    key = ["player_id", "policy"]
    days = g.assign(day=pd.to_datetime(g.ts).dt.date).groupby(key).day.nunique()
    per_player = who.set_index(key).assign(
        active_days=days,
        ad_rev=imp.groupby(key).revenue_usd.sum() if len(imp) else 0.0,
        spend=buys.groupby(key).price_usd.sum() if len(buys) else 0.0,
    ).fillna({"active_days": 0, "ad_rev": 0.0, "spend": 0.0}).reset_index()
    per_player["total_rev"] = per_player.ad_rev + per_player.spend

    rows = []
    for key, pp in per_player.groupby(by):
        key = key if isinstance(key, tuple) else (key,)
        sel = dict(zip(by, key))

        def f(df):
            m = pd.Series(True, index=df.index)
            for k, v in sel.items():
                m &= df[k] == v
            return df[m]

        gm, ov, ro, se, st, im, lb = map(f, (games, overs, offers, sess, starts, imp, lab))
        hours = se.duration_s.sum() / 3600 if len(se) else 0
        night = st.local_hour.isin([22, 23, 0, 1, 2]).mean() if len(st) else 0
        rows.append({**sel,
                     "jugadores": len(pp),
                     "días_activos": round(pp.active_days.mean(), 2),
                     "partidas/jugador": round(len(gm) / len(pp), 1),
                     "puntaje_mediano": int(gm.final_score.median()) if len(gm) else 0,
                     "forzados/100_partidas": round(100 * lb.forced.sum() / max(len(gm), 1), 1),
                     "derrotas_tras_récord_%": round(100 * ov.above_record.mean(), 1) if len(ov) else 0,
                     "acepta_revive_%": round(100 * ro.accepted.mean(), 1) if len(ro) else 0,
                     "anuncios/hora": round(len(im) / hours, 1) if hours else 0,
                     "sesiones_nocturnas_%": round(100 * night, 1),
                     "ingreso_anuncios_usd": round(pp.ad_rev.sum(), 2),
                     "compras_usd": round(pp.spend.sum(), 2),
                     "ingreso_top5%_%": round(100 * top_share(pp.total_rev), 1)})
    return pd.DataFrame(rows)


def main(argv=None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    roots = argv or ["../data"]
    parts = [load(r) for r in roots]
    d = {k: pd.concat([p[k] for p in parts], ignore_index=True) for k in parts[0]}
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 30)
    print("\n== Por política ==")
    print(metrics(d, ["policy"]).to_string(index=False))
    print("\n== Por política y perfil ==")
    print(metrics(d, ["policy", "profile"]).to_string(index=False))
    lab = d["labels"]
    print("\n== Repartos por tipo y regla ==")
    print(lab.groupby(["policy", "kind", lab.rule.fillna("-")]).size().to_string())


if __name__ == "__main__":
    main()

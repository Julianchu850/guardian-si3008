"""Línea de comandos del simulador.

Ejemplos:
  # Una semana, 300 jugadores, mitad en cada política, a archivos locales
  python -m guardian_sim --players 300 --days 7 --policy split --out ../data

  # El mismo mundo en las dos políticas (para el análisis "dos mundos")
  python -m guardian_sim --policy neutral   --out ../data/neutral
  python -m guardian_sim --policy predatory --out ../data/predatory

  # Publicar en Kafka a 1 hora simulada por segundo (etapa 2)
  python -m guardian_sim --sink kafka --bootstrap localhost:9092 --speed 3600
"""
from __future__ import annotations

import argparse
import time
from concurrent.futures import ProcessPoolExecutor

from . import engine
from .simulation import (OFFERS, create_players, day_start_ts, iso,
                         simulate_player_day)
from .sinks import FileSink, KafkaSink, MultiSink


def _chunk_day(args):
    players, day, start, interventions = args
    out_players, events, labels = [], [], []
    for p in players:
        p, e, lab = simulate_player_day(p, day, start, interventions)
        out_players.append(p)
        events.extend(e)
        labels.extend(lab)
    return out_players, events, labels


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="guardian_sim", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--players", type=int, default=300)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--policy", choices=["neutral", "predatory", "guarded", "split"], default="split",
                    help="guarded = juego depredador con el guardián embebido (requiere --guardian-model)")
    ap.add_argument("--guardian-model", default="",
                    help="carpeta local o s3://… con model.joblib (etapa 4)")
    ap.add_argument("--guardian-threshold", type=float, default=None,
                    help="umbral de riesgo; por defecto el elegido al entrenar")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start-date", default="2026-11-16")
    ap.add_argument("--out", default="../data")
    ap.add_argument("--sink", choices=["files", "kafka", "both"], default="files")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--speed", type=float, default=3600.0,
                    help="segundos simulados por segundo real en Kafka (0 = sin pausa)")
    ap.add_argument("--workers", type=int, default=2)
    args = ap.parse_args(argv)

    run_id = f"{args.policy}-s{args.seed}"
    # Las dimensiones batch y la verdad terreno siempre van a --out: no viajan por Kafka.
    # Los eventos van a archivos, a Kafka o a ambos según --sink.
    files = FileSink(args.out, run_id, events=args.sink in ("files", "both"))
    kafka = KafkaSink(args.bootstrap, args.speed) if args.sink in ("kafka", "both") else None
    sink = MultiSink(*[s for s in (files, kafka) if s])

    interventions = None
    if args.policy == "guarded":
        if not args.guardian_model:
            ap.error("--policy guarded requiere --guardian-model")
        from .simulation import GuardianInterventions
        interventions = GuardianInterventions(args.guardian_model, args.guardian_threshold)

    players = create_players(args.players, args.policy, args.seed, args.days)
    files.write_table(f"batch/players/part-{run_id}.jsonl",
                      [p.observable(day_start_ts(args.start_date, p.install_day)) for p in players])
    files.write_table("batch/catalog/pieces.jsonl",
                      [{"piece_id": p.id, "name": p.name, "size": p.size,
                        "height": p.height, "width": p.width} for p in engine.PIECES])
    files.write_table("batch/catalog/offers.jsonl",
                      [{"offer_id": o, "price_usd": pr} for o, pr in OFFERS])
    files.write_table(f"truth/players_truth/part-{run_id}.jsonl", [p.truth() for p in players])

    t0 = time.time()
    n_chunks = max(1, args.workers * 4)
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for day in range(args.days):
            start = day_start_ts(args.start_date, day)
            chunks = [players[i::n_chunks] for i in range(n_chunks)]
            players, events, labels = [], [], []
            for ps, ev, lab in pool.map(_chunk_day, [(c, day, start, interventions) for c in chunks]):
                players.extend(ps)
                events.extend(ev)
                labels.extend(lab)
            players.sort(key=lambda p: p.player_id)
            events.sort(key=lambda e: (e["ts"], e["event_id"]))
            sink.write_events(events)
            sink.write_labels(labels)
            active = sum(1 for p in players if not p.churned)
            print(f"día {day + 1}/{args.days} ({iso(start)[:10]}): {len(events):,} eventos, "
                  f"{sum(lab['forced'] for lab in labels)} repartos forzados, "
                  f"{active} jugadores activos  [{time.time() - t0:.0f}s]", flush=True)
    sink.close()


if __name__ == "__main__":
    main()

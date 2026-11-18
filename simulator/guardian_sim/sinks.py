"""Destinos de los datos generados.

- FileSink: escribe la misma estructura que tendrá el data lake en S3
  (bronce particionado por fecha y hora). Sirve para desarrollar sin AWS y
  para generar el histórico que se carga en batch.
- KafkaSink: publica los eventos en Kafka con clave = player_id, para que
  todos los eventos de un jugador caigan en la misma partición y en orden.
  Reproduce el reloj simulado a una velocidad configurable.
"""
from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from pathlib import Path

from .simulation import iso

AD_EVENTS = {"ad_impression", "reward_offer", "store_offer", "purchase"}
TOPIC_GAME = "game-events"
TOPIC_ADS = "ad-events"


def topic_for(event: dict) -> str:
    return TOPIC_ADS if event["event_type"] in AD_EVENTS else TOPIC_GAME


def serialize(event: dict) -> dict:
    out = dict(event)
    out["ts"] = iso(event["ts"])
    return out


class FileSink:
    def __init__(self, out_dir: str | Path, run_id: str, events: bool = True):
        self.root = Path(out_dir)
        self.run_id = run_id
        self.events = events

    def _append(self, path: Path, rows) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

    def write_events(self, events: list[dict]) -> None:
        if not self.events:
            return
        parts: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
        for e in events:
            s = serialize(e)
            parts[(topic_for(e), s["ts"][:10], s["ts"][11:13])].append(s)
        for (topic, dt, hr), rows in parts.items():
            self._append(self.root / "bronze" / topic / f"dt={dt}" / f"hr={hr}"
                         / f"part-{self.run_id}.jsonl", rows)

    def write_labels(self, labels: list[dict]) -> None:
        parts: dict[str, list[dict]] = defaultdict(list)
        for lab in labels:
            s = serialize(lab)
            parts[s["ts"][:10]].append(s)
        for dt, rows in parts.items():
            self._append(self.root / "truth" / "deal_labels" / f"dt={dt}"
                         / f"part-{self.run_id}.jsonl", rows)

    def write_table(self, rel_path: str, rows: list[dict]) -> None:
        path = self.root / rel_path
        if path.exists():
            path.unlink()
        self._append(path, rows)

    def close(self) -> None:
        pass


class KafkaSink:
    """Publica en Kafka respetando el reloj simulado.

    speed = segundos simulados por segundo real (3600 = una hora por segundo).
    speed = 0 publica tan rápido como se pueda.
    """

    def __init__(self, bootstrap: str, speed: float = 3600.0, producer=None):
        if producer is None:
            try:
                from confluent_kafka import Producer
            except ImportError as exc:  # pragma: no cover
                raise SystemExit("Instala confluent-kafka: pip install confluent-kafka") from exc
            producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50,
                                 "compression.type": "gzip", "acks": "all",
                                 "client.id": os.environ.get("HOSTNAME", "guardian-sim")})
        self.producer = producer
        self.speed = speed
        self._t0_sim: float | None = None
        self._t0_real = 0.0

    def write_events(self, events: list[dict]) -> None:
        for e in events:
            if self.speed > 0:
                if self._t0_sim is None:
                    self._t0_sim, self._t0_real = e["ts"], time.monotonic()
                wait = (e["ts"] - self._t0_sim) / self.speed - (time.monotonic() - self._t0_real)
                if wait > 0:
                    time.sleep(wait)
            self.producer.produce(topic_for(e), key=e["player_id"],
                                  value=json.dumps(serialize(e), separators=(",", ":")))
            self.producer.poll(0)
        self.producer.flush()

    def write_labels(self, labels: list[dict]) -> None:
        pass  # la verdad terreno no viaja por el stream

    def write_table(self, rel_path: str, rows: list[dict]) -> None:
        pass

    def close(self) -> None:
        self.producer.flush()


class MultiSink:
    def __init__(self, *sinks):
        self.sinks = sinks

    def __getattr__(self, name):
        def call(*args, **kwargs):
            for s in self.sinks:
                getattr(s, name)(*args, **kwargs)
        return call

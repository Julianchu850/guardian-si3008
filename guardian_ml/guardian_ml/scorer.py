"""Guardián en streaming: Kafka (game-events, ad-events) → interventions.

Cada evento actualiza el estado del jugador; cada reparto se puntúa con el
modelo y, si supera el umbral, se publica una intervención en el tópico
"interventions" (clave = player_id). El bronze-sink archiva ese tópico en S3.

Orden de los eventos: game-events y ad-events son tópicos distintos, así que
Kafka no garantiza el orden ENTRE ellos. Cada evento trae su número de
secuencia por jugador (final de event_id), y un búfer de reordenamiento los
procesa en orden. Si falta un número por más de HOLD_SECONDS (por ejemplo,
porque el servicio arrancó a mitad de una sesión), sigue con el siguiente.

Variables de entorno:
  KAFKA_BOOTSTRAP   (kafka:29092)     MODEL_URI   s3://…/models/guardian/latest  o carpeta local
  KAFKA_GROUP       (guardian-scorer) THRESHOLD   opcional, sobrescribe el del modelo
  HOLD_SECONDS      (5)
"""
from __future__ import annotations

import heapq
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field

from .features import event_seq
from .guardian import Guardian
from .model import RiskModel

log = logging.getLogger("guardian_scorer")
TOPICS_IN = ["game-events", "ad-events"]
TOPIC_OUT = "interventions"


@dataclass
class _Buf:
    heap: list = field(default_factory=list)   # (seq, llegada, evento)
    expected: int | None = None


class ReorderBuffer:
    """Entrega los eventos de cada jugador en orden de secuencia."""

    def __init__(self, hold_seconds: float = 5.0, clock=time.monotonic):
        self.hold = hold_seconds
        self.clock = clock
        self.bufs: dict[tuple, _Buf] = {}
        self.skipped = 0

    def push(self, event: dict) -> list[dict]:
        key = (event["player_id"], event["policy"])
        b = self.bufs.setdefault(key, _Buf())
        seq = event_seq(event["event_id"])
        if b.expected is not None and seq < b.expected:
            return []  # duplicado o ya procesado
        heapq.heappush(b.heap, (seq, self.clock(), event))
        if b.expected is None and seq == 1:
            b.expected = 1
        return self._drain(b)

    def _drain(self, b: _Buf) -> list[dict]:
        out = []
        while b.heap and b.expected is not None and b.heap[0][0] <= b.expected:
            seq, _, e = heapq.heappop(b.heap)
            if seq == b.expected:
                out.append(e)
                b.expected += 1
        return out

    def flush_stale(self) -> list[dict]:
        """Libera los búferes que esperan un hueco hace más de hold segundos."""
        now = self.clock()
        out = []
        for b in self.bufs.values():
            while b.heap and now - min(t for _, t, _ in b.heap) >= self.hold:
                seq = b.heap[0][0]
                if b.expected is not None and seq > b.expected:
                    self.skipped += seq - b.expected
                b.expected = seq
                out.extend(self._drain(b))
        return out


class Scorer:
    def __init__(self, guardian: Guardian, producer, hold_seconds: float = 5.0, clock=time.monotonic):
        self.guardian = guardian
        self.producer = producer
        self.buffer = ReorderBuffer(hold_seconds, clock)
        self.events = 0
        self.interventions = 0

    def handle(self, event: dict) -> None:
        self._process(self.buffer.push(event))

    def tick(self) -> None:
        self._process(self.buffer.flush_stale())

    def _process(self, events: list[dict]) -> None:
        for e in events:
            self.events += 1
            for iv in self.guardian.observe(e):
                self.interventions += 1
                self.producer.produce(TOPIC_OUT, key=iv["player_id"],
                                      value=json.dumps(iv, separators=(",", ":")))
        self.producer.poll(0)


def run(consumer, scorer: Scorer, should_stop, poll_timeout: float = 0.5) -> None:
    last_log = time.monotonic()
    try:
        while not should_stop():
            msg = consumer.poll(poll_timeout)
            if msg is not None and not msg.error():
                try:
                    scorer.handle(json.loads(msg.value()))
                except (ValueError, KeyError) as exc:
                    log.warning("evento ilegible: %s", exc)
            scorer.tick()
            if time.monotonic() - last_log > 30:
                last_log = time.monotonic()
                log.info("eventos=%d intervenciones=%d huecos_saltados=%d", scorer.events,
                         scorer.interventions, scorer.buffer.skipped)
    finally:
        scorer.producer.flush()
        consumer.close()


def main() -> None:
    from confluent_kafka import Consumer, Producer
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    uri = os.environ["MODEL_URI"]
    model = RiskModel.load(uri)
    threshold = float(os.environ["THRESHOLD"]) if os.environ.get("THRESHOLD") else None
    guardian = Guardian(model, threshold)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
    consumer = Consumer({"bootstrap.servers": bootstrap,
                         "group.id": os.environ.get("KAFKA_GROUP", "guardian-scorer"),
                         "auto.offset.reset": os.environ.get("OFFSET_RESET", "latest"),
                         "enable.auto.commit": True})
    consumer.subscribe(TOPICS_IN)
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "acks": "all"})
    scorer = Scorer(guardian, producer, float(os.environ.get("HOLD_SECONDS", 5)))
    log.info("guardián listo: modelo %s, umbral %.4f, %s → %s", model.version, guardian.threshold,
             TOPICS_IN, TOPIC_OUT)
    stop = {"flag": False}
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.update(flag=True))
    run(consumer, scorer, lambda: stop["flag"])


if __name__ == "__main__":
    main()

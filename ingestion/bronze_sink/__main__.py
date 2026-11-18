"""Arranca el consumidor bronze-sink. Se configura con variables de entorno:

KAFKA_BOOTSTRAP      servidores de Kafka (por defecto kafka:29092)
KAFKA_GROUP          grupo de consumidores (por defecto bronze-sink)
TOPICS               tópicos separados por coma (game-events,ad-events)
BRONZE_URI           s3://bucket/prefijo  o  ruta local (/data/lake)
FLUSH_MAX_RECORDS    mensajes por lote (5000)
FLUSH_MAX_SECONDS    segundos máximos por lote (60)
"""
from __future__ import annotations

import logging
import os
import signal

from confluent_kafka import Consumer

from .sink import Batcher, run, writer_from_uri


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092"),
        "group.id": os.environ.get("KAFKA_GROUP", "bronze-sink"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    topics = os.environ.get("TOPICS", "game-events,ad-events").split(",")
    consumer.subscribe(topics)
    writer = writer_from_uri(os.environ.get("BRONZE_URI", "/data/lake"))
    batcher = Batcher(max_records=int(os.environ.get("FLUSH_MAX_RECORDS", 5000)),
                      max_seconds=float(os.environ.get("FLUSH_MAX_SECONDS", 60)))

    stop = {"flag": False}

    def _stop(*_):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    logging.info("bronze-sink escuchando %s → %s", topics, os.environ.get("BRONZE_URI"))
    run(consumer, writer, batcher, lambda: stop["flag"])


if __name__ == "__main__":
    main()

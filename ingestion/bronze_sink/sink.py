"""Consumidor Kafka → capa bronce del data lake (S3 o disco local).

Garantía: al menos una vez (at-least-once). El flujo de cada lote es
  1. leer mensajes y agruparlos por (tópico, partición, fecha, hora) del evento;
  2. escribir cada grupo como un archivo JSONL comprimido con gzip;
  3. solo después de escribir todo, confirmar (commit) los offsets en Kafka.

Si una escritura falla no se confirma nada: al reiniciar, el consumidor vuelve
a leer desde el último offset confirmado. Puede haber duplicados en bronce;
la capa plata los elimina por event_id.

La partición del data lake usa la hora del EVENTO (campo ts), no la hora de
llegada, para que los datos del simulador y del juego web queden en el día
correcto aunque lleguen tarde. Los mensajes ilegibles van a _dead_letter/.
"""
from __future__ import annotations

import gzip
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("bronze_sink")


# --------------------------------------------------------------------------
# Destinos
# --------------------------------------------------------------------------
class Writer(ABC):
    @abstractmethod
    def write(self, key: str, data: bytes) -> None: ...


class LocalWriter(Writer):
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def write(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(path)          # escritura atómica


class S3Writer(Writer):
    def __init__(self, bucket: str, prefix: str = "", client=None):
        if client is None:
            import boto3
            client = boto3.client("s3")
        self.s3 = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def write(self, key: str, data: bytes) -> None:
        full = f"{self.prefix}/{key}" if self.prefix else key
        self.s3.put_object(Bucket=self.bucket, Key=full, Body=data,
                           ContentType="application/x-ndjson", ContentEncoding="gzip")


def writer_from_uri(uri: str) -> Writer:
    """s3://bucket/prefijo  o  una ruta local (file:///ruta también vale)."""
    if uri.startswith("s3://"):
        bucket, _, prefix = uri[5:].partition("/")
        return S3Writer(bucket, prefix)
    return LocalWriter(uri.removeprefix("file://"))


# --------------------------------------------------------------------------
# Lotes
# --------------------------------------------------------------------------
@dataclass
class _Group:
    lines: list[bytes] = field(default_factory=list)
    first_offset: int = -1
    last_offset: int = -1


@dataclass
class Batcher:
    max_records: int = 5000
    max_seconds: float = 60.0
    clock: callable = time.monotonic
    groups: dict[tuple, _Group] = field(default_factory=dict)
    offsets: dict[tuple[str, int], int] = field(default_factory=dict)  # (tópico, partición) -> último
    count: int = 0
    opened_at: float | None = None

    def add(self, topic: str, partition: int, offset: int, value: bytes | None) -> None:
        if self.opened_at is None:
            self.opened_at = self.clock()
        key = _route(topic, partition, value)
        g = self.groups.setdefault(key, _Group())
        g.lines.append(value or b"")
        if g.first_offset < 0:
            g.first_offset = offset
        g.last_offset = offset
        self.offsets[(topic, partition)] = max(offset, self.offsets.get((topic, partition), -1))
        self.count += 1

    def due(self) -> bool:
        if not self.count:
            return False
        return self.count >= self.max_records or self.clock() - self.opened_at >= self.max_seconds

    def drain(self) -> tuple[list[tuple[str, bytes]], dict[tuple[str, int], int]]:
        """Devuelve los archivos a escribir y los offsets a confirmar, y vacía el lote."""
        files = []
        for (kind, topic, partition, dt, hr), g in sorted(self.groups.items()):
            base = "bronze" if kind == "ok" else "bronze/_dead_letter"
            key = (f"{base}/{topic}/dt={dt}/hr={hr}/"
                   f"part-{topic}-p{partition}-o{g.first_offset:012d}-{g.last_offset:012d}.jsonl.gz")
            files.append((key, gzip.compress(b"\n".join(g.lines) + b"\n")))
        commits = {tp: off + 1 for tp, off in self.offsets.items()}
        self.groups, self.offsets, self.count, self.opened_at = {}, {}, 0, None
        return files, commits


def _route(topic: str, partition: int, value: bytes | None) -> tuple:
    try:
        ts = json.loads(value)["ts"]
        dt, hr = ts[:10], ts[11:13]
        datetime.strptime(f"{dt} {hr}", "%Y-%m-%d %H")
        return ("ok", topic, partition, dt, hr)
    except Exception:  # JSON inválido, sin ts o con formato raro
        now = datetime.now(timezone.utc)
        return ("dead", topic, partition, now.strftime("%Y-%m-%d"), now.strftime("%H"))


# --------------------------------------------------------------------------
# Bucle del consumidor
# --------------------------------------------------------------------------
def flush(batcher: Batcher, writer: Writer, consumer) -> int:
    files, commits = batcher.drain()
    if not files:
        return 0
    for key, data in files:
        writer.write(key, data)
    from confluent_kafka import TopicPartition
    consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in sorted(commits.items())],
                    asynchronous=False)
    log.info("lote escrito: %d archivos, offsets %s", len(files), commits)
    return len(files)


def run(consumer, writer: Writer, batcher: Batcher, should_stop, poll_timeout: float = 1.0) -> None:
    """Consume hasta que should_stop() sea verdadero; al salir vacía el último lote."""
    try:
        while not should_stop():
            msg = consumer.poll(poll_timeout)
            if msg is not None:
                if msg.error():
                    log.warning("error de Kafka: %s", msg.error())
                else:
                    batcher.add(msg.topic(), msg.partition(), msg.offset(), msg.value())
            if batcher.due():
                flush(batcher, writer, consumer)
        flush(batcher, writer, consumer)
    finally:
        consumer.close()

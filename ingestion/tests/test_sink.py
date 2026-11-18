import gzip
import json

import boto3
import pytest
from moto import mock_aws

from bronze_sink.sink import Batcher, LocalWriter, S3Writer, flush, run, writer_from_uri


class FakeMsg:
    def __init__(self, topic, partition, offset, value):
        self._t, self._p, self._o, self._v = topic, partition, offset, value

    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def value(self): return self._v
    def error(self): return None


class FakeConsumer:
    def __init__(self, msgs):
        self.msgs = list(msgs)
        self.commits = []
        self.closed = False

    def poll(self, timeout):
        return self.msgs.pop(0) if self.msgs else None

    def commit(self, offsets, asynchronous):
        self.commits.append({(tp.topic, tp.partition): tp.offset for tp in offsets})

    def close(self):
        self.closed = True


def ev(ts, i=0, etype="piece_deal"):
    return json.dumps({"event_id": f"p1-{i}", "event_type": etype, "ts": ts,
                       "player_id": "p1"}).encode()


def read_all(root):
    out = {}
    for f in sorted(root.rglob("*.jsonl.gz")):
        out[str(f.relative_to(root))] = gzip.decompress(f.read_bytes()).decode().splitlines()
    return out


def test_routes_by_event_time_and_commits_next_offsets(tmp_path):
    msgs = [FakeMsg("game-events", 0, 10, ev("2026-11-16T21:59:59.000Z", 1)),
            FakeMsg("game-events", 0, 11, ev("2026-11-16T22:00:01.000Z", 2)),
            FakeMsg("game-events", 3, 5, ev("2026-11-16T22:10:00.000Z", 3)),
            FakeMsg("ad-events", 1, 7, ev("2026-11-16T22:30:00.000Z", 4, "ad_impression"))]
    c = FakeConsumer(msgs)
    polls = iter(range(10))
    run(c, LocalWriter(tmp_path), Batcher(max_records=100), lambda: next(polls) >= 6)

    files = read_all(tmp_path)
    assert set(files) == {
        "bronze/game-events/dt=2026-11-16/hr=21/part-game-events-p0-o000000000010-000000000010.jsonl.gz",
        "bronze/game-events/dt=2026-11-16/hr=22/part-game-events-p0-o000000000011-000000000011.jsonl.gz",
        "bronze/game-events/dt=2026-11-16/hr=22/part-game-events-p3-o000000000005-000000000005.jsonl.gz",
        "bronze/ad-events/dt=2026-11-16/hr=22/part-ad-events-p1-o000000000007-000000000007.jsonl.gz",
    }
    assert c.commits == [{("ad-events", 1): 8, ("game-events", 0): 12, ("game-events", 3): 6}]
    assert c.closed


def test_flushes_when_batch_is_full(tmp_path):
    msgs = [FakeMsg("game-events", 0, i, ev("2026-11-16T10:00:00.000Z", i)) for i in range(5)]
    c = FakeConsumer(msgs)
    polls = iter(range(10))
    run(c, LocalWriter(tmp_path), Batcher(max_records=2), lambda: next(polls) >= 7)
    assert [list(x.values())[0] for x in c.commits] == [2, 4, 5]
    lines = [line for v in read_all(tmp_path).values() for line in v]
    assert len(lines) == 5


def test_flushes_by_time():
    now = {"t": 0.0}
    b = Batcher(max_records=1000, max_seconds=60, clock=lambda: now["t"])
    b.add("game-events", 0, 1, ev("2026-11-16T10:00:00.000Z"))
    assert not b.due()
    now["t"] = 61
    assert b.due()


def test_bad_messages_go_to_dead_letter(tmp_path):
    b = Batcher()
    b.add("game-events", 0, 1, b"esto no es json")
    b.add("game-events", 0, 2, json.dumps({"sin": "ts"}).encode())
    c = FakeConsumer([])
    flush(b, LocalWriter(tmp_path), c)
    keys = list(read_all(tmp_path))
    assert len(keys) == 1 and keys[0].startswith("bronze/_dead_letter/game-events/")
    assert c.commits == [{("game-events", 0): 3}]


def test_no_commit_if_write_fails():
    class Broken:
        def write(self, key, data):
            raise OSError("S3 no disponible")

    b = Batcher()
    b.add("game-events", 0, 1, ev("2026-11-16T10:00:00.000Z"))
    c = FakeConsumer([])
    with pytest.raises(OSError):
        flush(b, Broken(), c)
    assert c.commits == []


@mock_aws
def test_s3_writer_and_uri():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="guardian-lake")
    w = writer_from_uri("s3://guardian-lake/guardian")
    assert isinstance(w, S3Writer) and w.bucket == "guardian-lake" and w.prefix == "guardian"
    w.s3 = s3
    b = Batcher()
    b.add("game-events", 2, 40, ev("2026-11-16T10:00:00.000Z"))
    flush(b, w, FakeConsumer([]))
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="guardian-lake")["Contents"]]
    assert keys == ["guardian/bronze/game-events/dt=2026-11-16/hr=10/"
                    "part-game-events-p2-o000000000040-000000000040.jsonl.gz"]
    body = s3.get_object(Bucket="guardian-lake", Key=keys[0])["Body"].read()
    assert json.loads(gzip.decompress(body))["event_id"] == "p1-0"

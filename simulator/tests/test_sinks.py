import json

from guardian_sim.simulation import create_players, day_start_ts, simulate_player_day
from guardian_sim.sinks import TOPIC_ADS, TOPIC_GAME, KafkaSink


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value):
        self.sent.append((topic, key, json.loads(value)))

    def poll(self, timeout):
        pass

    def flush(self):
        pass


def test_kafka_sink_keys_by_player_and_splits_topics():
    events = []
    for p in create_players(3, "predatory", 1, 1):
        events += simulate_player_day(p, 0, day_start_ts("2026-11-16", 0))[1]
    events.sort(key=lambda e: e["ts"])
    fake = FakeProducer()
    KafkaSink("unused:9092", speed=0, producer=fake).write_events(events)

    assert len(fake.sent) == len(events)
    for topic, key, value in fake.sent:
        assert key == value["player_id"]
        assert value["ts"].endswith("Z")
        is_ad = value["event_type"] in {"ad_impression", "reward_offer", "store_offer", "purchase"}
        assert topic == (TOPIC_ADS if is_ad else TOPIC_GAME)
    # Por jugador, el orden de envío respeta el orden temporal.
    for pid in {k for _, k, _ in fake.sent}:
        ts = [v["ts"] for _, k, v in fake.sent if k == pid]
        assert ts == sorted(ts)

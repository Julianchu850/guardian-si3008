import json
import random

import numpy as np
import pandas as pd
import pytest

from guardian_ml.features import FEATURES, LABEL, PlayerState, board_fill, features_from_events
from guardian_ml.guardian import Guardian
from guardian_ml.model import RiskModel, load_bundle, save_bundle
from guardian_ml.scorer import ReorderBuffer, Scorer
from guardian_ml.train import baseline_rule, split_by_time, train


def ev(seq, et, pid="p1-00001", policy="predatory", **kw):
    return {"event_id": f"{pid}-{seq}", "event_type": et, "player_id": pid, "policy": policy,
            "ts": f"2026-11-16T10:00:{seq % 60:02d}.000Z", "session_id": f"{pid}-s1",
            "game_id": kw.pop("game_id", f"{pid}-g1"), **kw}


def test_player_state_features():
    st = PlayerState()
    for e in [ev(1, "game_start", game_num=3),
              ev(2, "ad_impression", ad_type="rewarded", completed=True, ecpm=10.0),
              ev(3, "ad_impression", ad_type="rewarded", completed=False, ecpm=12.0),
              ev(4, "ad_impression", ad_type="interstitial", completed=True, ecpm=99.0),
              ev(5, "turn_result", turn_ms=4000), ev(6, "turn_result", turn_ms=2000),
              ev(7, "revive")]:
        st.update(e)
    f = st.features(score=6000, high_score=5000, loaded_ecpm=36.0, deal_num=9, board_hex="ff")
    assert f["rewarded_last3_completed"] == 1 and f["rewarded_count"] == 2
    assert f["last_rewarded_ecpm"] == 12.0 and f["ecpm_ratio"] == 3.0
    assert f["above_record"] == 1.0 and f["score_ratio"] == 1.2
    assert f["avg_turn_s_last6"] == 3.0 and f["revives_in_game"] == 1 and f["game_num"] == 3
    assert f["board_fill"] == 8 and set(f) == set(FEATURES)
    assert board_fill("ffffffffffffffff") == 64


def test_features_from_events_use_only_the_past():
    events = [ev(1, "game_start", game_num=1),
              ev(2, "piece_deal", deal_id="d1", score=0, high_score=0, rewarded_ecpm_loaded=5.0,
                 deal_num=1, board="0"),
              ev(3, "ad_impression", ad_type="rewarded", completed=True, ecpm=5.0),
              ev(4, "piece_deal", deal_id="d2", score=10, high_score=0, rewarded_ecpm_loaded=20.0,
                 deal_num=2, board="0")]
    rows = {r["deal_id"]: r for r in features_from_events(list(reversed(events)))}
    assert rows["d1"]["rewarded_count"] == 0 and rows["d2"]["rewarded_count"] == 1
    assert rows["d2"]["ecpm_ratio"] == 4.0


# --------------------------------------------------------------------------
@pytest.fixture(scope="module")
def bundle(tmp_path_factory):
    """Modelo pequeño entrenado con datos sintéticos que siguen la regla depredadora."""
    rng = np.random.default_rng(0)
    n = 30000
    df = pd.DataFrame({f: rng.uniform(0, 2, n) for f in FEATURES})
    df["above_record"] = (rng.random(n) < 0.2).astype(float)
    df["ecpm_ratio"] = rng.uniform(0, 6, n)
    df["rewarded_last3_completed"] = rng.integers(0, 4, n).astype(float)
    df["game_num"] = rng.integers(1, 10, n).astype(float)
    df["score_ratio"] = rng.uniform(0, 1.3, n)
    df[LABEL] = baseline_rule(df) & (rng.random(n) < 0.9)
    df["dt"] = np.repeat(["2026-11-16", "2026-11-17", "2026-11-18", "2026-11-19"], n // 4)
    df["policy"] = "predatory"
    b = train(df, target_recall=0.9)
    out = save_bundle(b, tmp_path_factory.mktemp("model"))
    return b, out


def test_train_split_and_metrics(bundle):
    b, _ = bundle
    assert b["data"]["test_dates"] == ["2026-11-18", "2026-11-19"]
    assert b["test"]["recall"] >= 0.8 and b["test"]["pr_auc"] > 0.5
    assert b["importance"][0][0] in {"above_record", "ecpm_ratio", "score_ratio",
                                     "rewarded_last3_completed", "game_num"}


def test_split_needs_positives():
    df = pd.DataFrame({"dt": ["a", "b"], LABEL: [False, False], "policy": "predatory"})
    with pytest.raises(ValueError, match="No hay repartos forzados"):
        split_by_time(df)


def test_saved_model_loads_and_fast_path_matches(bundle):
    b, path = bundle
    load_bundle.cache_clear()
    m = RiskModel.load(str(path))
    assert m.fast is not None and m.version == b["version"]
    assert json.loads((path / "metrics.json").read_text())["threshold"] == b["threshold"]
    rng = np.random.default_rng(1)
    for _ in range(50):
        f = {k: float(v) for k, v in zip(FEATURES, rng.uniform(0, 5, len(FEATURES)))}
        ref = m.model.predict_proba(np.array([[f[k] for k in FEATURES]]))[0, 1]
        assert m.risk(f) == pytest.approx(ref, abs=1e-12)


# --------------------------------------------------------------------------
class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value):
        self.sent.append((topic, key, json.loads(value)))

    def poll(self, t):
        pass

    def flush(self):
        pass


def _risky_session(pid="p1-00001"):
    """Sesión donde el contexto coincide con la regla depredadora."""
    es = [ev(1, "session_start", pid=pid), ev(2, "game_start", pid=pid, game_num=4)]
    for i in range(3):
        es.append(ev(3 + i, "ad_impression", pid=pid, ad_type="rewarded", completed=True, ecpm=10.0))
    es.append(ev(6, "piece_deal", pid=pid, deal_id=f"{pid}-g1-d9", score=6000, high_score=5000,
                 rewarded_ecpm_loaded=40.0, deal_num=9, board="0f0f0f0f00000000"))
    es.append(ev(7, "game_over", pid=pid, above_record=True))
    es.append(ev(8, "game_over", pid=pid, above_record=True))
    es.append(ev(9, "purchase", pid=pid, price_usd=0.99))
    es.append(ev(10, "purchase", pid=pid, price_usd=0.99))
    return es


def test_guardian_emits_the_three_interventions(bundle):
    g = Guardian(RiskModel(bundle[0]), threshold=0.5)
    out = [iv for e in _risky_session() for iv in g.observe(e)]
    assert [iv["action"] for iv in out] == ["block_manipulation", "suggest_break", "spend_limit"]
    assert out[0]["risk"] >= 0.5 and out[0]["trigger_event_id"] == "p1-00001-6"


def test_reorder_buffer_restores_order_across_topics():
    now = {"t": 0.0}
    buf = ReorderBuffer(hold_seconds=5, clock=lambda: now["t"])
    events = [ev(i, "turn_result", turn_ms=1) for i in range(1, 21)]
    shuffled = events[:]
    random.Random(3).shuffle(shuffled)
    got = [e for x in shuffled for e in buf.push(x)]
    assert [e["event_id"] for e in got] == [e["event_id"] for e in events]
    assert buf.push(events[4]) == []           # duplicado: se ignora


def test_reorder_buffer_skips_gap_after_hold():
    now = {"t": 0.0}
    buf = ReorderBuffer(hold_seconds=5, clock=lambda: now["t"])
    assert buf.push(ev(1, "x")) != [] and buf.push(ev(3, "x")) == []
    now["t"] = 6
    assert [e["event_id"] for e in buf.flush_stale()] == ["p1-00001-3"]
    assert buf.skipped == 1
    # Arranque a mitad de sesión: el primer evento visto no es el 1
    assert buf.push(ev(40, "x", pid="p2-00002")) == []
    now["t"] = 12
    assert [e["event_id"] for e in buf.flush_stale()] == ["p2-00002-40"]


def test_scorer_publishes_interventions_in_order_even_if_shuffled(bundle):
    g = Guardian(RiskModel(bundle[0]), threshold=0.5)
    prod = FakeProducer()
    sc = Scorer(g, prod, hold_seconds=5)
    session = _risky_session()
    for e in [session[i] for i in (1, 0, 5, 2, 4, 3, 6, 7, 9, 8)]:
        sc.handle(e)
    assert [v["action"] for _, _, v in prod.sent] == ["block_manipulation", "suggest_break", "spend_limit"]
    assert all(t == "interventions" and k == "p1-00001" for t, k, _ in prod.sent)
    assert sc.events == 10

from guardian_sim.profiles import Traits
from guardian_sim.simulation import create_players, day_start_ts, simulate_player_day

LATENT = set(Traits.__dataclass_fields__) | {"profile", "forced", "kind", "rule"}
COMMON = {"event_id", "event_type", "ts", "player_id", "session_id", "game_id",
          "policy", "source", "schema_version"}


def _run(policy, seed=11, n=6, days=2):
    events, labels = [], []
    for p in create_players(n, policy, seed, days):
        for d in range(days):
            p, e, lab = simulate_player_day(p, d, day_start_ts("2026-11-16", d))
            events += e
            labels += lab
    return events, labels


def test_deterministic_with_same_seed():
    a, _ = _run("predatory")
    b, _ = _run("predatory")
    assert a == b and len(a) > 100


def test_events_have_schema_and_no_latent_fields():
    events, labels = _run("predatory")
    for e in events:
        assert COMMON <= set(e), e
        assert not (LATENT & set(e)), e
    assert {lab["deal_id"] for lab in labels} == {
        e["deal_id"] for e in events if e["event_type"] == "piece_deal"}


def test_split_is_stratified_by_profile():
    players = create_players(400, "split", 3, 7)
    by = {}
    for p in players:
        by.setdefault(p.profile, []).append(p.policy)
    for pols in by.values():
        assert abs(pols.count("predatory") - pols.count("neutral")) <= 1

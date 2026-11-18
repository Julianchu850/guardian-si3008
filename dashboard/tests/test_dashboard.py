import gzip
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from guardian_dashboard import queries as q
from guardian_dashboard.backends import DuckDBBackend

APP = Path(__file__).resolve().parents[1] / "guardian_dashboard" / "app.py"


def _write(df: pd.DataFrame, path: Path, partition: str | None = "dt"):
    if partition:
        for v, g in df.groupby(partition):
            d = path / f"{partition}={v}"
            d.mkdir(parents=True, exist_ok=True)
            g.drop(columns=[partition]).to_parquet(d / "part-0.parquet", index=False)
    else:
        path.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path / "part-0.parquet", index=False)


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    rows = []
    for pol, rev, ads in (("neutral", 0.2, 20), ("predatory", 0.4, 30), ("guarded", 0.3, 30)):
        for i in range(20):
            for dt in ("2026-11-16", "2026-11-17"):
                rows.append(dict(player_id=f"p42-{i:05d}", policy=pol, dt=dt, sessions=2,
                                 night_sessions=1 if i < 5 else 0, play_seconds=3600.0, games=10,
                                 deals=100, max_score=5000, new_records=1, game_overs=10,
                                 game_overs_above_record=2 if pol == "predatory" else 1,
                                 revive_offers=4, revives_accepted=2, interventions=0,
                                 ad_impressions=ads, rewarded_shown=3, rewarded_completed=3,
                                 ad_revenue_usd=rev if i else rev * 30, purchases=0, purchases_usd=0.0,
                                 post_loss_offers=0, total_revenue_usd=rev if i else rev * 30))
    _write(pd.DataFrame(rows), root / "gold/player_daily")
    labels = []
    for pol, forced, suppressed in (("neutral", 0, 0), ("predatory", 8, 0), ("guarded", 1, 7)):
        for k in range(40):
            labels.append(dict(deal_id=f"{pol}-{k}", player_id=f"p42-{k % 20:05d}", game_id="g",
                               policy=pol, kind="lethal" if k < forced else "normal",
                               rule="record_margin" if k < forced else None,
                               suppressed_rule="ad_harvest" if forced <= k < forced + suppressed else None,
                               forced=k < forced, solvable=k >= forced, dt="2026-11-16"))
    _write(pd.DataFrame(labels), root / "silver/deal_labels")
    truth = pd.DataFrame([dict(player_id=f"p42-{i:05d}", policy=pol,
                               profile="casual" if i % 2 else "whale")
                          for pol in ("neutral", "predatory", "guarded") for i in range(20)])
    _write(truth, root / "silver/players_truth", None)
    ev = pd.DataFrame([dict(event_id=f"e{i}", event_type="intervention_applied", player_id="p42-00001",
                            policy="guarded", action="block_manipulation", dt="2026-11-16")
                       for i in range(3)])
    _write(ev, root / "silver/game_events")
    d = root / "bronze/interventions/dt=2026-11-16/hr=10"
    d.mkdir(parents=True)
    with gzip.open(d / "part-0.jsonl.gz", "wt") as f:
        for i, a in enumerate(["block_manipulation", "suggest_break", "block_manipulation"]):
            f.write(json.dumps({"intervention_id": f"x{i}", "ts": f"2026-11-16T10:00:0{i}.000Z",
                                "player_id": "p42-00003", "policy": "predatory", "action": a,
                                "risk": 0.5 if a == "block_manipulation" else None,
                                "reason": "r", "model_version": "v1"}) + "\n")
    (root / "model-latest").mkdir()
    (root / "model-latest/metrics.json").write_text(json.dumps({
        "version": "v1", "threshold": 0.1,
        "data": {"test_rows": 1000, "test_dates": ["2026-11-17"], "test_positives": 10},
        "test": {"precision": 0.2, "recall": 0.9, "pr_auc": 0.5, "confusion": {"tp": 9, "fn": 1, "fp": 36}},
        "baseline_rule_test": {"precision": 0.03, "recall": 1.0, "confusion": {"tp": 10, "fn": 0, "fp": 300}},
        "importance": [["ecpm_ratio", 0.4], ["score_ratio", 0.3]]}))
    return root


def test_world_metrics(lake):
    b = DuckDBBackend(str(lake))
    f = q.Filters(date(2026, 11, 16), date(2026, 11, 17), "p42")
    m = q.world_metrics(b.query(q.sql_worlds(f)), b.query(q.sql_forced_by_rule(f)),
                        b.query(q.sql_player_revenue(f)))
    assert list(m.index) == ["neutral", "predatory", "guarded"]
    assert m.loc["predatory", "anuncios_hora"] == pytest.approx(30.0)
    assert m.loc["predatory", "letales_100_partidas"] == pytest.approx(100 * 8 / 400)
    assert m.loc["guarded", "bloqueados"] == 7
    assert m.loc["predatory", "derrotas_tras_record"] == pytest.approx(0.2)
    # 1 de 20 jugadores (el 5 %) aporta 30 de 49 partes del ingreso
    assert m.loc["neutral", "ingreso_top5"] == pytest.approx(60 / 98)


def test_filters_are_validated():
    with pytest.raises(ValueError):
        q.Filters(date(2026, 1, 1), date(2026, 1, 2), "p42'; DROP TABLE x;--").where()
    w = q.Filters(date(2026, 1, 1), date(2026, 1, 2), None, ("guarded", "bogus")).where()
    assert "policy IN ('guarded')" in w and "LIKE" not in w


def test_profiles_and_interventions(lake):
    b = DuckDBBackend(str(lake))
    f = q.Filters(date(2026, 11, 16), date(2026, 11, 17), "p42")
    p = q.profile_metrics(b.query(q.sql_by_profile(f)), b.query(q.sql_forced_by_profile(f)))
    assert p.groupby("policy").participacion_ingreso.sum().round(6).eq(1).all()
    assert b.query(q.sql_interventions_embedded(f)).intervenciones.sum() == 3
    s = b.query(q.sql_interventions_stream()).set_index("accion")
    assert s.loc["block_manipulation", "intervenciones"] == 2


def test_app_renders_without_errors(lake, monkeypatch):
    from streamlit.testing.v1 import AppTest
    monkeypatch.setenv("GUARDIAN_BACKEND", "duckdb")
    monkeypatch.setenv("GUARDIAN_LAKE", str(lake))
    # El lake de prueba no tiene todas las tablas: se omite el estado del data lake
    monkeypatch.setattr(q, "sql_freshness", lambda: "SELECT 'ok' AS tabla")
    at = AppTest.from_file(str(APP), default_timeout=60).run()
    assert not at.exception, at.exception
    assert [s.value for s in at.subheader][:2] == ["Resumen", "Ingreso vs. daño"]
    labels = [m.label for m in at.metric]
    assert "Manos letales / 100 partidas" in labels and "Bloquear manipulación" in labels


def test_athena_backend_parses_types(monkeypatch):
    """Simula las respuestas de Athena para probar la conversión de tipos."""
    from guardian_dashboard import backends

    class FakeAthena:
        def __init__(self):
            self.sql = []

        def start_query_execution(self, **kw):
            self.sql.append(kw["QueryString"])
            assert kw["ResultConfiguration"]["OutputLocation"] == "s3://b/guardian/athena-results/"
            return {"QueryExecutionId": "q1"}

        def get_query_execution(self, QueryExecutionId):
            return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

        def get_paginator(self, name):
            class P:
                def paginate(self, QueryExecutionId):
                    cols = [{"Name": "policy", "Type": "varchar"}, {"Name": "n", "Type": "bigint"},
                            {"Name": "x", "Type": "double"}, {"Name": "ok", "Type": "boolean"},
                            {"Name": "dt", "Type": "date"}]
                    hdr = {"Data": [{"VarCharValue": c["Name"]} for c in cols]}
                    row = {"Data": [{"VarCharValue": v} for v in ("guarded", "3", "0.5", "true", "2026-11-16")]}
                    yield {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": cols}, "Rows": [hdr, row]}}
                    yield {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": cols},
                                         "Rows": [{"Data": [{"VarCharValue": "neutral"}, {"VarCharValue": "1"},
                                                            {}, {"VarCharValue": "false"},
                                                            {"VarCharValue": "2026-11-17"}]}]}}
            return P()

    fake = FakeAthena()

    class FakeBoto:
        @staticmethod
        def client(name, region_name=None):
            return fake if name == "athena" else object()

    monkeypatch.setitem(__import__("sys").modules, "boto3", FakeBoto)
    monkeypatch.setenv("GUARDIAN_LAKE", "s3://b/guardian")
    monkeypatch.delenv("ATHENA_OUTPUT", raising=False)
    monkeypatch.setenv("GUARDIAN_BACKEND", "athena")
    b = backends.from_env()
    assert "CREATE EXTERNAL TABLE IF NOT EXISTS guardian.bronze_interventions" in fake.sql[0]
    df = b.query("SELECT 1")
    assert df.n.tolist() == [3, 1] and df.ok.tolist() == [True, False]
    assert df.x.iloc[0] == 0.5 and pd.isna(df.x.iloc[1]) and df.dt.iloc[0] == date(2026, 11, 16)

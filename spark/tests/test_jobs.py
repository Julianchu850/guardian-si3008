"""Prueba de punta a punta de los jobs con Spark local.

Genera un día simulado, corre bronce→plata→oro y compara las cifras de oro con
las calculadas directamente sobre los eventos crudos. Requiere pyspark 3.5 y
Java; si no están, se omite.
"""
import gzip
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
pd = pytest.importorskip("pandas")

ROOT = Path(__file__).resolve().parents[2]
JOBS = ROOT / "spark" / "jobs"
SPARK_SUBMIT = Path(pyspark.__file__).parent / "bin" / "spark-submit"
DT = "2026-11-16"


def _submit(script, *args, cwd):
    env = {**os.environ, "PYSPARK_PYTHON": sys.executable}
    subprocess.run([str(SPARK_SUBMIT), "--master", "local[2]", "--conf", "spark.ui.enabled=false",
                    "--py-files", str(JOBS / "common.py"), str(JOBS / script), *args],
                   check=True, cwd=cwd, env=env, capture_output=True)


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    if shutil.which("java") is None:
        pytest.skip("se necesita Java para Spark")
    lake = tmp_path_factory.mktemp("lake")
    subprocess.run([sys.executable, "-m", "guardian_sim", "--players", "30", "--days", "1",
                    "--policy", "split", "--seed", "5", "--start-date", DT, "--out", str(lake),
                    "--workers", "1"], check=True, cwd=ROOT / "simulator", capture_output=True)
    # Basura a propósito: JSON roto y un duplicado exacto de un evento real.
    hr = sorted((lake / "bronze/game-events" / f"dt={DT}").iterdir())[0]
    first = next(hr.glob("*.jsonl")).read_text().splitlines()[0]
    with gzip.open(hr / "part-basura.jsonl.gz", "wt") as f:
        f.write('{"event_id": roto\n' + first + "\n")
    _submit("bronze_to_silver.py", "--lake", str(lake), "--dates", DT, "--dims", cwd=lake)
    _submit("silver_to_gold.py", "--lake", str(lake), "--dates", DT, cwd=lake)
    return lake


def _raw(lake, topic):
    rows = []
    for f in (lake / "bronze" / topic / f"dt={DT}").rglob("part-*.jsonl"):
        rows += [json.loads(line) for line in f.read_text().splitlines()]
    return pd.DataFrame(rows)


def test_quality_report_catches_garbage(lake):
    part = next((lake / "quality/bronze_to_silver" / f"dt={DT}").glob("part-*"))
    report = json.loads(part.read_text())
    game = next(t for t in report["topics"] if t["topic"] == "game-events")
    assert game["invalid_by_reason"] == {"json_invalido": 1}
    assert game["duplicates"] == 1
    assert game["valid"] == len(_raw(lake, "game-events"))


def test_gold_matches_raw_events(lake):
    ads = _raw(lake, "ad-events")
    games = _raw(lake, "game-events")
    harm = pd.read_parquet(lake / "gold/harm_daily").set_index("policy")
    imp = ads[ads.event_type == "ad_impression"].groupby("policy").revenue_usd.sum()
    ends = games[games.event_type == "game_end"].groupby("policy").size()
    for pol in harm.index:
        assert harm.loc[pol, "ad_revenue_usd"] == pytest.approx(imp.get(pol, 0.0))
        assert harm.loc[pol, "games"] == ends.get(pol, 0)
    assert set(harm.index) == {"neutral", "predatory"}


def test_silver_has_dimensions_and_labels(lake):
    for table in ("players", "pieces", "offers", "players_truth", "deal_labels"):
        assert (lake / "silver" / table).exists(), table
    labels = pd.read_parquet(lake / "silver/deal_labels")
    assert labels[labels.policy == "neutral"].forced.sum() == 0

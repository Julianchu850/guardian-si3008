import json
import time

import boto3
import pytest
from moto import mock_aws

import guardian_lib as lib

LAKE = "s3://guardian-lake/guardian"


@pytest.fixture
def s3():
    with mock_aws():
        c = boto3.client("s3", region_name="us-east-1")
        c.create_bucket(Bucket="guardian-lake")
        yield c


def put(c, key, body=b"x"):
    c.put_object(Bucket="guardian-lake", Key=f"guardian/{key}", Body=body)


def test_detect_dates_is_incremental(s3):
    put(s3, "bronze/game-events/dt=2026-11-16/hr=10/a.jsonl.gz")
    put(s3, "bronze/ad-events/dt=2026-11-17/hr=10/a.jsonl.gz")
    put(s3, "silver/game_events/dt=2026-11-16/part-0.parquet")
    assert lib.detect_dates(s3, LAKE) == ["2026-11-17"]          # la 16 ya está en plata
    time.sleep(1.1)
    put(s3, "bronze/game-events/dt=2026-11-16/hr=23/tarde.jsonl.gz")  # llegó un evento tarde
    assert lib.detect_dates(s3, LAKE) == ["2026-11-16", "2026-11-17"]


def test_sync_dir_uploads_only_new_or_changed(s3, tmp_path):
    run = tmp_path / "historico-split-s42"
    (run / "batch/players").mkdir(parents=True)
    (run / "batch/players/part-split-s42.jsonl").write_text('{"player_id":"p1"}\n')
    (run / "bronze/game-events/dt=2026-11-16/hr=10").mkdir(parents=True)
    (run / "bronze/game-events/dt=2026-11-16/hr=10/part-split-s42.jsonl").write_text("{}\n")
    (run / "otra_cosa.txt").write_text("no se sube")
    assert lib.sync_dir(s3, tmp_path, LAKE) == 2
    assert lib.sync_dir(s3, tmp_path, LAKE) == 0                 # nada cambió
    (run / "batch/players/part-split-s42.jsonl").write_text('{"player_id":"p1"}\n{"player_id":"p2"}\n')
    assert lib.sync_dir(s3, tmp_path, LAKE) == 1
    keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket="guardian-lake")["Contents"])
    assert keys == ["guardian/batch/players/part-split-s42.jsonl",
                    "guardian/bronze/game-events/dt=2026-11-16/hr=10/part-split-s42.jsonl"]


def test_upload_code(s3, tmp_path):
    (tmp_path / "common.py").write_text("")
    (tmp_path / "bronze_to_silver.py").write_text("")
    assert lib.upload_code(s3, tmp_path, LAKE) == "s3://guardian-lake/guardian/code"
    keys = {o["Key"] for o in s3.list_objects_v2(Bucket="guardian-lake")["Contents"]}
    assert keys == {"guardian/code/common.py", "guardian/code/bronze_to_silver.py"}


def test_job_flow_config_and_steps():
    cfg = lib.job_flow_config(LAKE, "emr-7.2.0", "m5.xlarge", 2, subnet="subnet-1")
    assert cfg["Instances"]["InstanceGroups"][1]["InstanceCount"] == 2
    assert cfg["Instances"]["Ec2SubnetId"] == "subnet-1" and "Ec2KeyName" not in cfg["Instances"]
    assert cfg["AutoTerminationPolicy"]["IdleTimeout"] == 3600
    assert cfg["LogUri"] == "s3://guardian-lake/guardian/emr-logs/"
    steps = lib.spark_steps(LAKE, "s3://guardian-lake/guardian/code", ["2026-11-16", "2026-11-17"])
    args = steps[0]["HadoopJarStep"]["Args"]
    assert args[:5] == ["spark-submit", "--deploy-mode", "cluster", "--py-files",
                        "s3://guardian-lake/guardian/code/common.py"]
    assert "--dims" in args and args[args.index("--dates") + 1] == "2026-11-16,2026-11-17"
    assert "--dims" not in steps[1]["HadoopJarStep"]["Args"]
    assert all(s["ActionOnFailure"] == "CONTINUE" for s in steps)


def test_quality(s3):
    report = {"dt": "2026-11-16", "topics": [
        {"topic": "game-events", "read": 1000, "valid": 980, "invalid": 20,
         "duplicates": 0, "invalid_by_reason": {"json_invalido": 20}},
        {"topic": "ad-events", "read": 0, "valid": 0, "invalid": 0, "duplicates": 0,
         "invalid_by_reason": {}}]}
    put(s3, "quality/bronze_to_silver/dt=2026-11-16/part-00000-abc.txt", json.dumps(report).encode())
    put(s3, "quality/bronze_to_silver/dt=2026-11-16/_SUCCESS")
    reports = lib.read_quality(s3, LAKE, ["2026-11-16"])
    assert reports == [report]
    assert lib.quality_problems(reports, ["2026-11-16"], 0.05) == []
    probs = lib.quality_problems(reports, ["2026-11-16", "2026-11-17"], 0.01)
    assert len(probs) == 2 and "2026-11-17" in probs[0] and "2.0%" in probs[1]

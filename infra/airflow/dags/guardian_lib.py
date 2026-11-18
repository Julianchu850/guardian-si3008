"""Lógica de los DAGs de Guardián, separada de Airflow para poder probarla.

Todas las funciones reciben un cliente de boto3 (en Airflow sale de S3Hook),
así que en las pruebas se usan con un S3 simulado (moto).
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path

DT_RE = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})/")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"se esperaba s3://bucket/prefijo y llegó {uri!r}")
    bucket, _, prefix = uri[5:].partition("/")
    return bucket, prefix.strip("/")


def _join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p)


# --------------------------------------------------------------------------
# ¿Qué fechas hay que procesar?
# --------------------------------------------------------------------------
def partition_dates(client, bucket: str, prefix: str) -> dict[str, datetime]:
    """Fechas dt=… bajo un prefijo, con la última modificación de cada una."""
    out: dict[str, datetime] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            m = DT_RE.search("/" + obj["Key"])
            if m:
                dt = m.group(1)
                if dt not in out or obj["LastModified"] > out[dt]:
                    out[dt] = obj["LastModified"]
    return out


def dates_to_process(bronze: dict[str, datetime], silver: dict[str, datetime]) -> list[str]:
    """Procesamiento incremental: fechas nuevas o con datos de bronce más
    recientes que su última versión en plata (llegaron eventos tarde)."""
    return sorted(dt for dt, mod in bronze.items() if dt not in silver or mod > silver[dt])


def detect_dates(client, lake: str) -> list[str]:
    bucket, prefix = parse_s3_uri(lake)
    bronze = {}
    for topic in ("game-events", "ad-events"):
        for dt, mod in partition_dates(client, bucket, _join(prefix, "bronze", topic)).items():
            bronze[dt] = max(mod, bronze.get(dt, mod))
    silver = partition_dates(client, bucket, _join(prefix, "silver", "game_events"))
    return dates_to_process(bronze, silver)


# --------------------------------------------------------------------------
# Carga batch: carpeta local → S3
# --------------------------------------------------------------------------
def sync_dir(client, local_root: str | Path, lake: str,
             subdirs=("batch", "truth", "bronze")) -> int:
    """Sube los archivos que no están en S3 o cambiaron de tamaño. Devuelve cuántos subió."""
    bucket, prefix = parse_s3_uri(lake)
    root = Path(local_root)
    existing: dict[str, int] = {}
    for sub in subdirs:
        for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=_join(prefix, sub) + "/"):
            for obj in page.get("Contents", []):
                existing[obj["Key"]] = obj["Size"]
    uploaded = 0
    for run_dir in sorted(p for p in root.iterdir() if p.is_dir()) if root.exists() else []:
        for sub in subdirs:
            base = run_dir / sub
            if not base.exists():
                continue
            for f in sorted(base.rglob("*.jsonl")):
                key = _join(prefix, sub, str(f.relative_to(base)))
                if existing.get(key) != f.stat().st_size:
                    client.upload_file(str(f), bucket, key)
                    uploaded += 1
    return uploaded


def upload_code(client, code_dir: str | Path, lake: str) -> str:
    bucket, prefix = parse_s3_uri(lake)
    for f in sorted(Path(code_dir).glob("*.py")):
        client.upload_file(str(f), bucket, _join(prefix, "code", f.name))
    return f"s3://{bucket}/{_join(prefix, 'code')}"


# --------------------------------------------------------------------------
# EMR
# --------------------------------------------------------------------------
def job_flow_config(lake: str, release: str, instance_type: str, core_count: int,
                    subnet: str = "", key_name: str = "", run_name: str = "guardian-etl",
                    service_role: str = "EMR_DefaultRole",
                    job_flow_role: str = "EMR_EC2_DefaultRole") -> dict:
    instances: dict = {
        "InstanceGroups": [
            {"Name": "Primario", "Market": "ON_DEMAND", "InstanceRole": "MASTER",
             "InstanceType": instance_type, "InstanceCount": 1},
            {"Name": "Core", "Market": "ON_DEMAND", "InstanceRole": "CORE",
             "InstanceType": instance_type, "InstanceCount": int(core_count)},
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    }
    if subnet:
        instances["Ec2SubnetId"] = subnet
    if key_name:
        instances["Ec2KeyName"] = key_name
    return {
        "Name": run_name,
        "ReleaseLabel": release,
        "Applications": [{"Name": "Spark"}],
        "LogUri": f"{lake.rstrip('/')}/emr-logs/",
        "Instances": instances,
        "JobFlowRole": job_flow_role,
        "ServiceRole": service_role,
        "VisibleToAllUsers": True,
        # Red de seguridad para los créditos del laboratorio: si Airflow no
        # alcanza a apagarlo, el clúster se apaga solo tras 1 hora sin trabajo.
        "AutoTerminationPolicy": {"IdleTimeout": 3600},
        "Configurations": [
            {"Classification": "spark-hive-site", "Properties": {
                "hive.metastore.client.factory.class":
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}},
            {"Classification": "spark-defaults", "Properties": {
                "spark.sql.sources.partitionOverwriteMode": "dynamic"}},
        ],
        "Tags": [{"Key": "proyecto", "Value": "guardian"}],
    }


def spark_steps(lake: str, code_uri: str, dates: list[str], glue: bool = True,
                dims: bool = True) -> list[dict]:
    common = ["--lake", lake.rstrip("/"), "--dates", ",".join(dates),
              "--catalog", "glue" if glue else "none"]

    def step(name: str, script: str, extra: list[str]) -> dict:
        return {"Name": f"{name} {dates[0]}…{dates[-1]}",
                "ActionOnFailure": "CONTINUE",   # Airflow decide y apaga el clúster
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": [
                    "spark-submit", "--deploy-mode", "cluster",
                    "--py-files", f"{code_uri}/common.py", f"{code_uri}/{script}",
                    *common, *extra]}}

    return [step("bronce→plata", "bronze_to_silver.py", ["--dims"] if dims else []),
            step("plata→oro", "silver_to_gold.py", []),
            step("variables guardián", "deal_features.py", [])]


# --------------------------------------------------------------------------
# Modelo del guardián
# --------------------------------------------------------------------------
def download_prefix(client, lake: str, rel_prefix: str, local_dir: str | Path,
                    dates: list[str] | None = None) -> int:
    """Descarga los Parquet de lake/rel_prefix (opcionalmente solo algunas fechas)."""
    bucket, prefix = parse_s3_uri(lake)
    base = _join(prefix, rel_prefix) + "/"
    n = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=base):
        for obj in page.get("Contents", []):
            rel = obj["Key"][len(base):]
            if not rel.endswith(".parquet"):
                continue
            m = DT_RE.search("/" + rel)
            if dates and (not m or m.group(1) not in dates):
                continue
            dest = Path(local_dir) / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, obj["Key"], str(dest))
            n += 1
    return n


def publish_model(client, local_dir: str | Path, lake: str, version: str) -> list[str]:
    """Sube el modelo a models/guardian/v=<versión>/ y lo copia a latest/."""
    bucket, prefix = parse_s3_uri(lake)
    uris = []
    for target in (f"v={version}", "latest"):
        for f in sorted(Path(local_dir).iterdir()):
            key = _join(prefix, "models", "guardian", target, f.name)
            client.upload_file(str(f), bucket, key)
        uris.append(f"s3://{bucket}/{_join(prefix, 'models', 'guardian', target)}")
    return uris


# --------------------------------------------------------------------------
# Calidad
# --------------------------------------------------------------------------
def read_quality(client, lake: str, dates: list[str]) -> list[dict]:
    bucket, prefix = parse_s3_uri(lake)
    reports = []
    for dt in dates:
        base = _join(prefix, "quality", "bronze_to_silver", f"dt={dt}") + "/"
        for obj in client.list_objects_v2(Bucket=bucket, Prefix=base).get("Contents", []):
            if os.path.basename(obj["Key"]).startswith("part-"):
                body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                reports.extend(json.loads(line) for line in body.decode().splitlines() if line)
    return reports


def quality_problems(reports: list[dict], dates: list[str], max_invalid_ratio: float = 0.01) -> list[str]:
    problems = []
    seen = {r["dt"] for r in reports}
    for dt in dates:
        if dt not in seen:
            problems.append(f"{dt}: no hay reporte de calidad (¿falló el paso de Spark?)")
    for r in reports:
        for t in r["topics"]:
            if t["read"] == 0:
                continue
            ratio = t["invalid"] / t["read"]
            if ratio > max_invalid_ratio:
                problems.append(f"{r['dt']} {t['topic']}: {ratio:.1%} inválidos {t['invalid_by_reason']}")
            if t["valid"] == 0:
                problems.append(f"{r['dt']} {t['topic']}: ningún registro válido")
    return problems

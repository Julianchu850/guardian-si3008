"""De dónde salen los datos del dashboard.

- AthenaBackend: producción. Consulta el Glue Data Catalog (base "guardian")
  con Athena; el modelo se lee de S3.
- DuckDBBackend: desarrollo y pruebas. Lee el mismo data lake en una carpeta
  local con DuckDB, con las mismas tablas y el mismo SQL.

El SQL de queries.py está escrito para funcionar igual en los dos.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pandas as pd

TABLES = {
    # tabla: (carpeta en el lake, ¿particionada por dt?)
    "gold_player_daily": ("gold/player_daily", True),
    "gold_harm_daily": ("gold/harm_daily", True),
    "gold_forced_deals_daily": ("gold/forced_deals_daily", True),
    "gold_deal_features": ("gold/deal_features", True),
    "silver_game_events": ("silver/game_events", True),
    "silver_ad_events": ("silver/ad_events", True),
    "silver_deal_labels": ("silver/deal_labels", True),
    "silver_players_truth": ("silver/players_truth", False),
    "silver_players": ("silver/players", False),
}

INTERVENTIONS_DDL = """
CREATE EXTERNAL TABLE IF NOT EXISTS {db}.bronze_interventions (
  intervention_id string, ts string, player_id string, policy string, session_id string,
  game_id string, trigger_event_id string, action string, risk double, threshold double,
  reason string, model_version string)
PARTITIONED BY (dt string, hr string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{lake}/bronze/interventions/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.dt.type'='date', 'projection.dt.format'='yyyy-MM-dd',
  'projection.dt.range'='2026-01-01,NOW+2YEARS', 'projection.dt.interval'='1',
  'projection.dt.interval.unit'='DAYS',
  'projection.hr.type'='integer', 'projection.hr.range'='0,23', 'projection.hr.digits'='2',
  'storage.location.template'='{lake}/bronze/interventions/dt=${{dt}}/hr=${{hr}}/')
"""


class AthenaBackend:
    name = "Athena"

    def __init__(self, database: str, lake: str, output: str, workgroup: str = "primary",
                 region: str | None = None):
        import boto3
        self.athena = boto3.client("athena", region_name=region)
        self.s3 = boto3.client("s3", region_name=region)
        self.database, self.lake, self.output, self.workgroup = database, lake.rstrip("/"), output, workgroup
        self.query(INTERVENTIONS_DDL.format(db=database, lake=self.lake))

    def query(self, sql: str) -> pd.DataFrame:
        qid = self.athena.start_query_execution(
            QueryString=sql, QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output}, WorkGroup=self.workgroup,
        )["QueryExecutionId"]
        while True:
            st = self.athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(0.4)
        if st["State"] != "SUCCEEDED":
            raise RuntimeError(f"Athena: {st.get('StateChangeReason', st['State'])}")
        cols, rows = None, []
        for page in self.athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
            meta = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            data = page["ResultSet"]["Rows"]
            if cols is None:
                cols = [(c["Name"], c["Type"]) for c in meta]
                data = data[1:]  # la primera fila son los encabezados
            rows += [[d.get("VarCharValue") for d in r["Data"]] for r in data]
        df = pd.DataFrame(rows, columns=[c for c, _ in cols or []])
        for name, typ in cols or []:
            if typ in ("integer", "bigint", "smallint", "tinyint", "double", "float", "real", "decimal"):
                df[name] = pd.to_numeric(df[name])
            elif typ == "boolean":
                df[name] = df[name].map({"true": True, "false": False})
            elif typ == "date":
                df[name] = pd.to_datetime(df[name]).dt.date
        return df

    def model_metrics(self) -> dict | None:
        bucket, _, prefix = self.lake[5:].partition("/")
        try:
            body = self.s3.get_object(Bucket=bucket,
                                      Key=f"{prefix}/models/guardian/latest/metrics.json")["Body"]
            return json.loads(body.read())
        except Exception:
            return None


class DuckDBBackend:
    name = "DuckDB (local)"

    def __init__(self, lake: str, model_dir: str | None = None):
        import duckdb
        self.lake = Path(lake)
        self.model_dir = Path(model_dir) if model_dir else self.lake / "model-latest"
        self.con = duckdb.connect()
        self.con.execute("CREATE SCHEMA IF NOT EXISTS guardian")
        for table, (rel, partitioned) in TABLES.items():
            path = self.lake / rel
            if not path.exists():
                continue
            glob = f"{path}/*/*.parquet" if partitioned else f"{path}/*.parquet"
            self.con.execute(f"CREATE OR REPLACE VIEW guardian.{table} AS SELECT * FROM "
                             f"read_parquet('{glob}', hive_partitioning=true)")
        iv = self.lake / "bronze/interventions"
        if iv.exists():
            self.con.execute(
                "CREATE OR REPLACE VIEW guardian.bronze_interventions AS SELECT * FROM "
                f"read_json_auto('{iv}/*/*/*.jsonl.gz', hive_partitioning=true, "
                "hive_types={'dt': VARCHAR, 'hr': VARCHAR})")
        else:
            self.con.execute(
                "CREATE OR REPLACE VIEW guardian.bronze_interventions AS SELECT "
                "NULL::VARCHAR AS action, NULL::VARCHAR AS player_id, NULL::DOUBLE AS risk, "
                "NULL::VARCHAR AS ts, NULL::VARCHAR AS reason, NULL::VARCHAR AS policy, "
                "NULL::VARCHAR AS model_version WHERE false")

    def query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()

    def model_metrics(self) -> dict | None:
        f = self.model_dir / "metrics.json"
        return json.loads(f.read_text()) if f.exists() else None


def from_env():
    kind = os.environ.get("GUARDIAN_BACKEND", "athena").lower()
    lake = os.environ["GUARDIAN_LAKE"]
    if kind == "duckdb":
        return DuckDBBackend(lake, os.environ.get("GUARDIAN_MODEL_DIR"))
    return AthenaBackend(
        database=os.environ.get("ATHENA_DATABASE", "guardian"), lake=lake,
        output=os.environ.get("ATHENA_OUTPUT") or lake.rstrip("/") + "/athena-results/",
        workgroup=os.environ.get("ATHENA_WORKGROUP", "primary"),
        region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

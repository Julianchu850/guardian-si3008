"""Piezas compartidas por los jobs de Spark (se envía a EMR con --py-files).

Compatible con Spark 3.5 (EMR 7.x).
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import types as T

TOPICS = {"game-events": "game_events", "ad-events": "ad_events"}

GAME_EVENT_TYPES = ["session_start", "game_start", "piece_deal", "turn_result", "game_over",
                    "revive_offer", "revive_response", "revive", "game_end",
                    "intervention_applied", "session_end"]
AD_EVENT_TYPES = ["ad_impression", "reward_offer", "store_offer", "purchase"]

_COMMON = [
    ("event_id", T.StringType()), ("event_type", T.StringType()), ("ts", T.StringType()),
    ("player_id", T.StringType()), ("session_id", T.StringType()), ("game_id", T.StringType()),
    ("policy", T.StringType()), ("source", T.StringType()), ("schema_version", T.IntegerType()),
]
_GAME = [
    ("country", T.StringType()), ("platform", T.StringType()), ("device_tier", T.StringType()),
    ("local_hour", T.IntegerType()), ("game_num", T.IntegerType()), ("high_score", T.LongType()),
    ("deal_id", T.StringType()), ("deal_num", T.IntegerType()),
    ("pieces", T.ArrayType(T.IntegerType())), ("board", T.StringType()),
    ("placeable_now", T.IntegerType()), ("score", T.LongType()),
    ("rewarded_ecpm_loaded", T.DoubleType()), ("pieces_placed", T.IntegerType()),
    ("lines_cleared", T.IntegerType()), ("points", T.LongType()), ("turn_ms", T.LongType()),
    ("above_record", T.BooleanType()), ("revives_used", T.IntegerType()),
    ("accepted", T.BooleanType()), ("final_score", T.LongType()), ("new_record", T.BooleanType()),
    ("deals", T.IntegerType()), ("action", T.StringType()), ("games", T.IntegerType()),
    ("duration_s", T.DoubleType()),
]
_ADS = [
    ("ad_type", T.StringType()), ("placement", T.StringType()), ("ecpm", T.DoubleType()),
    ("revenue_usd", T.DoubleType()), ("completed", T.BooleanType()), ("watch_ms", T.LongType()),
    ("accepted", T.BooleanType()), ("rewarded_ecpm_loaded", T.DoubleType()),
    ("offer_id", T.StringType()), ("price_usd", T.DoubleType()), ("trigger", T.StringType()),
]
CORRUPT = "_corrupt_record"


def _schema(fields) -> T.StructType:
    return T.StructType([T.StructField(n, t, True) for n, t in fields]
                        + [T.StructField(CORRUPT, T.StringType(), True)])


SCHEMAS = {"game-events": _schema(_COMMON + _GAME), "ad-events": _schema(_COMMON + _ADS)}
VALID_TYPES = {"game-events": GAME_EVENT_TYPES, "ad-events": AD_EVENT_TYPES}


def spark_session(app: str, catalog: str) -> SparkSession:
    b = (SparkSession.builder.appName(app)
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.session.timeZone", "UTC"))
    if catalog == "glue":
        b = b.enableHiveSupport()  # en EMR el metastore es el Glue Data Catalog
    return b.getOrCreate()


def base_args(description: str) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument("--lake", required=True, help="raíz del data lake: s3://bucket/guardian o ruta local")
    ap.add_argument("--dates", required=True, help="fechas AAAA-MM-DD separadas por coma")
    ap.add_argument("--catalog", choices=["none", "glue"], default="none")
    ap.add_argument("--database", default="guardian")
    return ap


def register(spark: SparkSession, database: str, table: str, location: str,
             partitioned: bool = True) -> None:
    """Crea (si no existe) la tabla externa en el catálogo y refresca particiones."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {database}.{table} USING PARQUET LOCATION '{location}'")
    if partitioned:
        spark.sql(f"ALTER TABLE {database}.{table} RECOVER PARTITIONS")
    spark.sql(f"REFRESH TABLE {database}.{table}")


def write_json(spark: SparkSession, path: str, payload: dict) -> None:
    """Escribe un JSON pequeño (reportes de calidad) en S3 o disco."""
    (spark.createDataFrame([(json.dumps(payload, ensure_ascii=False),)], ["value"])
     .coalesce(1).write.mode("overwrite").text(path))

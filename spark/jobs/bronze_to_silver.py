"""Bronce → plata.

Por cada fecha:
  1. lee el JSONL crudo de bronce (game-events y ad-events) con esquema explícito;
  2. separa los registros inválidos (JSON dañado, campos clave nulos, tipo de
     evento desconocido, fecha que no corresponde) hacia una cuarentena;
  3. elimina duplicados por event_id (Kafka entrega al menos una vez);
  4. escribe Parquet particionado por dt, sobrescribiendo solo esa fecha
     (reprocesar una fecha es idempotente);
  5. deja un reporte de calidad que Airflow revisa.

Con --dims también refresca las dimensiones batch (jugadores, catálogo) y la
verdad terreno, que llegan por la ruta batch y no por Kafka.

Uso:
  spark-submit --py-files common.py bronze_to_silver.py \
      --lake s3://bucket/guardian --dates 2026-11-16,2026-11-17 --dims --catalog glue
"""
from __future__ import annotations

from pyspark.sql import functions as F

from common import (CORRUPT, SCHEMAS, TOPICS, VALID_TYPES, base_args, register,
                    spark_session, write_json)


def _exists(spark, path: str) -> bool:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


def process_topic(spark, lake: str, topic: str, dt: str) -> dict:
    src = f"{lake}/bronze/{topic}/dt={dt}"
    table = TOPICS[topic]
    if not _exists(spark, src):
        return {"dt": dt, "topic": topic, "read": 0, "valid": 0, "invalid": 0, "duplicates": 0,
                "invalid_by_reason": {}, "note": "sin datos en bronce"}

    raw = (spark.read.schema(SCHEMAS[topic])
           .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", CORRUPT)
           .json(f"{src}/hr=*/*")
           .withColumn("_source_file", F.input_file_name()))
    raw = raw.cache()  # necesario para filtrar por la columna de registros dañados

    ts = F.to_timestamp("ts")
    reason = (F.when(F.col(CORRUPT).isNotNull(), "json_invalido")
              .when(F.col("event_id").isNull() | F.col("player_id").isNull(), "llave_nula")
              .when(~F.col("event_type").isin(VALID_TYPES[topic]), "tipo_desconocido")
              .when(ts.isNull(), "ts_invalido")
              .when(F.to_date(ts) != F.lit(dt).cast("date"), "fecha_no_coincide")
              .when(~F.col("policy").isin("neutral", "predatory"), "politica_desconocida"))
    tagged = raw.withColumn("_reason", reason)

    bad = tagged.filter(F.col("_reason").isNotNull())
    good = (tagged.filter(F.col("_reason").isNull())
            .drop("_reason", CORRUPT)
            .withColumn("event_ts", ts)
            .withColumn("hr", F.hour(ts))
            .withColumn("dt", F.lit(dt)))
    # (event_id, policy): en el experimento "dos mundos" el mismo jugador existe en ambas políticas
    deduped = good.dropDuplicates(["event_id", "policy"])

    n_read = raw.count()
    by_reason = {r["_reason"]: r["count"] for r in bad.groupBy("_reason").count().collect()}
    n_good = good.count()
    n_valid = deduped.count()

    (deduped.drop("ts").repartition(4).write.mode("overwrite").partitionBy("dt")
     .parquet(f"{lake}/silver/{table}"))
    if by_reason:
        (bad.withColumn("dt", F.lit(dt)).write.mode("overwrite").partitionBy("dt")
         .parquet(f"{lake}/silver/_quarantine/{table}"))
    raw.unpersist()
    return {"dt": dt, "topic": topic, "read": n_read, "valid": n_valid,
            "invalid": n_read - n_good, "duplicates": n_good - n_valid,
            "invalid_by_reason": by_reason}


def load_dims(spark, lake: str) -> list[str]:
    """Dimensiones y verdad terreno que llegan por la ruta batch."""
    done = []
    sources = {
        "players": f"{lake}/batch/players",
        "pieces": f"{lake}/batch/catalog/pieces.jsonl",
        "offers": f"{lake}/batch/catalog/offers.jsonl",
        "players_truth": f"{lake}/truth/players_truth",
    }
    for table, path in sources.items():
        if _exists(spark, path):
            df = spark.read.json(path)
            df = df.dropDuplicates(["player_id", "policy"]) if "player_id" in df.columns else df
            df.coalesce(1).write.mode("overwrite").parquet(f"{lake}/silver/{table}")
            done.append(table)
    labels = f"{lake}/truth/deal_labels"
    if _exists(spark, labels):
        (spark.read.json(f"{labels}/dt=*/*")
         .withColumn("event_ts", F.to_timestamp("ts")).drop("ts")
         .withColumn("dt", F.to_date("event_ts").cast("string"))
         .dropDuplicates(["deal_id", "policy"])
         .write.mode("overwrite").partitionBy("dt").parquet(f"{lake}/silver/deal_labels"))
        done.append("deal_labels")
    return done


def main() -> None:
    ap = base_args(__doc__)
    ap.add_argument("--dims", action="store_true", help="refrescar dimensiones y verdad terreno")
    args = ap.parse_args()
    spark = spark_session("guardian-bronze-to-silver", args.catalog)
    dates = [d.strip() for d in args.dates.split(",") if d.strip()]

    dims = load_dims(spark, args.lake) if args.dims else []
    for dt in dates:
        report = {"dt": dt, "topics": [process_topic(spark, args.lake, t, dt) for t in TOPICS]}
        write_json(spark, f"{args.lake}/quality/bronze_to_silver/dt={dt}", report)
        print("CALIDAD", report, flush=True)

    if args.catalog == "glue":
        for table in TOPICS.values():
            register(spark, args.database, f"silver_{table}", f"{args.lake}/silver/{table}")
        for table in dims:
            register(spark, args.database, f"silver_{table}", f"{args.lake}/silver/{table}",
                     partitioned=table == "deal_labels")
    spark.stop()


if __name__ == "__main__":
    main()

"""Plata → oro: una fila por reparto con las variables del guardián y su etiqueta.

Reproduce con ventanas de Spark la misma definición que guardian_ml/features.py
(la que usa el servicio en streaming). Cada variable mira solo eventos ANTERIORES
al reparto, ordenados por la secuencia del jugador (el número final de event_id).

Salida: gold/deal_features (dt), con la etiqueta "forced" de la verdad terreno.

Uso:
  spark-submit --py-files common.py deal_features.py --lake s3://bucket/guardian --dates 2026-11-16
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import base_args, register, spark_session

PLAYER = ["player_id", "policy"]


def _popcount_hex64(col):
    hi = F.conv(F.substring(col, 1, 8), 16, 10).cast("long")
    lo = F.conv(F.substring(col, 9, 8), 16, 10).cast("long")
    return (F.bit_count(hi) + F.bit_count(lo)).cast("double")


def build(games, ads, labels):
    seq = F.substring_index("event_id", "-", -1).cast("long")
    cols = ["event_id", "event_type", "player_id", "policy", "game_id", "dt"]
    g = games.select(*cols, "game_num", "deal_id", "deal_num", "score", "high_score",
                     "rewarded_ecpm_loaded", "board", "turn_ms",
                     F.lit(None).cast("string").alias("ad_type"),
                     F.lit(None).cast("boolean").alias("completed"),
                     F.lit(None).cast("double").alias("ecpm"))
    a = ads.select(*cols, *[F.lit(None).cast(t).alias(c) for c, t in [
        ("game_num", "int"), ("deal_id", "string"), ("deal_num", "int"), ("score", "long"),
        ("high_score", "long"), ("rewarded_ecpm_loaded", "double"), ("board", "string"),
        ("turn_ms", "long")]], "ad_type", "completed", "ecpm")
    ev = g.unionByName(a).withColumn("seq", seq)

    # Historial de anuncios rewarded (por jugador, a lo largo de todas sus partidas)
    is_rew = (F.col("event_type") == "ad_impression") & (F.col("ad_type") == "rewarded")
    w_rew = Window.partitionBy(*PLAYER, F.when(is_rew, 1)).orderBy("seq")
    comp = F.col("completed").cast("int")
    ev = (ev.withColumn("r_last3", F.when(is_rew, comp + F.coalesce(F.lag(comp, 1).over(w_rew), F.lit(0))
                                         + F.coalesce(F.lag(comp, 2).over(w_rew), F.lit(0))))
            .withColumn("r_count", F.when(is_rew, F.row_number().over(w_rew)))
            .withColumn("r_ecpm", F.when(is_rew, F.col("ecpm"))))

    # Ventana por partida: últimas 6 manos y revives usados
    is_turn = F.col("event_type") == "turn_result"
    w_turn = Window.partitionBy(*PLAYER, "game_id", F.when(is_turn, 1)).orderBy("seq").rowsBetween(-5, 0)
    ev = ev.withColumn("t_avg6", F.when(is_turn, F.avg("turn_ms").over(w_turn)))

    before = Window.partitionBy(*PLAYER).orderBy("seq").rowsBetween(Window.unboundedPreceding, -1)
    before_game = (Window.partitionBy(*PLAYER, "game_id").orderBy("seq")
                   .rowsBetween(Window.unboundedPreceding, -1))
    ev = (ev.withColumn("f_last3", F.last("r_last3", ignorenulls=True).over(before))
            .withColumn("f_rcount", F.last("r_count", ignorenulls=True).over(before))
            .withColumn("f_lecpm", F.last("r_ecpm", ignorenulls=True).over(before))
            .withColumn("f_avg6", F.last("t_avg6", ignorenulls=True).over(before_game))
            .withColumn("f_revives", F.sum(F.when(F.col("event_type") == "revive", 1).otherwise(0))
                        .over(before_game))
            .withColumn("f_game_num", F.last(F.when(F.col("event_type") == "game_start",
                                                    F.col("game_num")), ignorenulls=True).over(before_game)))

    d = ev.where(F.col("event_type") == "piece_deal")
    score = F.coalesce(F.col("score"), F.lit(0)).cast("double")
    high = F.coalesce(F.col("high_score"), F.lit(0)).cast("double")
    loaded = F.coalesce(F.col("rewarded_ecpm_loaded"), F.lit(0.0))
    last = F.coalesce(F.col("f_lecpm"), F.lit(0.0))
    out = d.select(
        "deal_id", "player_id", "policy", "game_id", "dt",
        score.alias("score"),
        high.alias("high_score"),
        F.when(high > 0, score / high).otherwise(0.0).alias("score_ratio"),
        F.when((high > 0) & (score > high), 1.0).otherwise(0.0).alias("above_record"),
        F.when(high > 0, (score - high) / high).otherwise(0.0).alias("record_margin"),
        loaded.alias("loaded_ecpm"),
        last.alias("last_rewarded_ecpm"),
        F.when(last > 0, loaded / last).otherwise(0.0).alias("ecpm_ratio"),
        F.coalesce(F.col("f_last3"), F.lit(0)).cast("double").alias("rewarded_last3_completed"),
        F.coalesce(F.col("f_rcount"), F.lit(0)).cast("double").alias("rewarded_count"),
        F.coalesce(F.col("deal_num"), F.lit(0)).cast("double").alias("deal_num"),
        F.coalesce(F.col("f_game_num"), F.lit(0)).cast("double").alias("game_num"),
        F.coalesce(F.col("f_revives"), F.lit(0)).cast("double").alias("revives_in_game"),
        (F.coalesce(F.col("f_avg6"), F.lit(0.0)) / 1000.0).alias("avg_turn_s_last6"),
        F.when(F.col("board").isNotNull(), _popcount_hex64(F.col("board"))).otherwise(0.0)
        .alias("board_fill"),
    )
    if labels is not None:
        lab = labels.select("deal_id", "policy", "forced", "rule", "kind")
        out = out.join(lab, ["deal_id", "policy"], "left")
    return out


def main() -> None:
    args = base_args(__doc__).parse_args()
    spark = spark_session("guardian-deal-features", args.catalog)
    lake = args.lake
    dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    # Se lee TODO el historial de plata: las variables dependen de eventos anteriores
    # (por ejemplo, los últimos 3 anuncios pueden ser de días previos). Solo se
    # escriben las fechas pedidas.
    games = spark.read.parquet(f"{lake}/silver/game_events")
    ads = spark.read.parquet(f"{lake}/silver/ad_events")
    try:
        labels = spark.read.parquet(f"{lake}/silver/deal_labels")
    except Exception:  # sin verdad terreno (datos del juego web)
        labels = None
    feats = build(games, ads, labels).where(F.col("dt").isin(dates))
    (feats.repartition("dt").write.mode("overwrite").partitionBy("dt")
     .parquet(f"{lake}/gold/deal_features"))
    print("FEATURES", feats.groupBy("policy").agg(F.count("*").alias("deals"),
                                                  F.sum(F.col("forced").cast("int")).alias("forced"))
          .collect(), flush=True)
    if args.catalog == "glue":
        register(spark, args.database, "gold_deal_features", f"{lake}/gold/deal_features")
    spark.stop()


if __name__ == "__main__":
    main()

"""Plata → oro: métricas listas para el dashboard "dos mundos".

Tablas (particionadas por dt):
- player_daily: actividad, anuncios, gasto y señales de daño por jugador y día.
- harm_daily: las mismas métricas agregadas por política (neutral vs depredadora),
  incluida la concentración del ingreso en el 5 % que más aporta.
- forced_deals_daily: repartos forzados por regla, desde la verdad terreno.
  Es una tabla de AUDITORÍA del experimento: el estudio real no la tendría, y
  el guardián nunca la usa como variable.

Uso:
  spark-submit --py-files common.py silver_to_gold.py \
      --lake s3://bucket/guardian --dates 2026-11-16 --catalog glue
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import base_args, register, spark_session, write_json

KEY = ["player_id", "policy"]
NIGHT_HOURS = [22, 23, 0, 1, 2]


def _n(cond):
    """Cuenta las filas que cumplen la condición."""
    return F.sum(F.when(cond, 1).otherwise(0))


def player_daily(games, ads):
    et = F.col("event_type")
    g = games.groupBy(KEY).agg(
        _n(et == "session_start").alias("sessions"),
        _n((et == "session_start") & F.col("local_hour").isin(NIGHT_HOURS)).alias("night_sessions"),
        F.sum(F.when(et == "session_end", F.col("duration_s")).otherwise(0.0)).alias("play_seconds"),
        _n(et == "game_end").alias("games"),
        F.sum(F.when(et == "game_end", F.col("deals")).otherwise(0)).alias("deals"),
        F.max(F.when(et == "game_end", F.col("final_score"))).alias("max_score"),
        _n((et == "game_end") & F.col("new_record")).alias("new_records"),
        _n(et == "game_over").alias("game_overs"),
        _n((et == "game_over") & F.col("above_record")).alias("game_overs_above_record"),
        _n(et == "revive_response").alias("revive_offers"),
        _n((et == "revive_response") & F.col("accepted")).alias("revives_accepted"),
        _n(et == "intervention_applied").alias("interventions"),
    )
    a = ads.groupBy(KEY).agg(
        _n(et == "ad_impression").alias("ad_impressions"),
        _n((et == "ad_impression") & (F.col("ad_type") == "rewarded")).alias("rewarded_shown"),
        _n((et == "ad_impression") & (F.col("ad_type") == "rewarded") & F.col("completed"))
        .alias("rewarded_completed"),
        F.sum(F.when(et == "ad_impression", F.col("revenue_usd")).otherwise(0.0)).alias("ad_revenue_usd"),
        _n(et == "purchase").alias("purchases"),
        F.sum(F.when(et == "purchase", F.col("price_usd")).otherwise(0.0)).alias("purchases_usd"),
        _n((et == "store_offer") & (F.col("trigger") == "post_loss")).alias("post_loss_offers"),
    )
    out = g.join(a, KEY, "full_outer").fillna(0)
    return out.withColumn("total_revenue_usd", F.col("ad_revenue_usd") + F.col("purchases_usd"))


def harm_daily(pd_):
    w = Window.partitionBy("policy").orderBy(F.col("total_revenue_usd").desc())
    n = Window.partitionBy("policy")
    ranked = (pd_.withColumn("rn", F.row_number().over(w))
              .withColumn("n", F.count(F.lit(1)).over(n))
              .withColumn("top5", F.col("rn") <= F.greatest(F.lit(1), F.round(F.col("n") * 0.05))))
    s = F.sum
    agg = ranked.groupBy("policy").agg(
        F.count(F.lit(1)).alias("players"),
        s("sessions").alias("sessions"), s("games").alias("games"),
        (s("play_seconds") / 3600).alias("play_hours"),
        s("ad_impressions").alias("ad_impressions"),
        s("ad_revenue_usd").alias("ad_revenue_usd"), s("purchases_usd").alias("purchases_usd"),
        s("total_revenue_usd").alias("total_revenue_usd"),
        s(F.when(F.col("top5"), F.col("total_revenue_usd")).otherwise(0.0)).alias("_top5_rev"),
        s("game_overs").alias("game_overs"),
        s("game_overs_above_record").alias("game_overs_above_record"),
        s("revive_offers").alias("revive_offers"), s("revives_accepted").alias("revives_accepted"),
        s("night_sessions").alias("night_sessions"), s("post_loss_offers").alias("post_loss_offers"),
    )

    def ratio(a, b):
        return F.when(F.col(b) > 0, F.col(a) / F.col(b))

    return (agg
            .withColumn("revenue_top5_share", ratio("_top5_rev", "total_revenue_usd"))
            .withColumn("ads_per_hour", ratio("ad_impressions", "play_hours"))
            .withColumn("loss_after_record_rate", ratio("game_overs_above_record", "game_overs"))
            .withColumn("revive_accept_rate", ratio("revives_accepted", "revive_offers"))
            .withColumn("night_session_share", ratio("night_sessions", "sessions"))
            .withColumn("revenue_per_player_usd", F.col("total_revenue_usd") / F.col("players"))
            .drop("_top5_rev"))


def forced_deals_daily(labels):
    return (labels.groupBy("policy", F.coalesce("rule", F.lit("none")).alias("rule"), "kind")
            .agg(F.count(F.lit(1)).alias("deals"),
                 _n(F.col("forced")).alias("forced"),
                 _n(F.col("suppressed_rule").isNotNull()).alias("suppressed"),
                 _n(~F.col("solvable")).alias("unsolvable")))


def main() -> None:
    args = base_args(__doc__).parse_args()
    spark = spark_session("guardian-silver-to-gold", args.catalog)
    lake = args.lake
    dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    games_all = spark.read.parquet(f"{lake}/silver/game_events")
    ads_all = spark.read.parquet(f"{lake}/silver/ad_events")
    try:
        labels_all = spark.read.parquet(f"{lake}/silver/deal_labels")
    except Exception:  # sin verdad terreno (por ejemplo, datos del juego web)
        labels_all = None

    for dt in dates:
        games = games_all.where(F.col("dt") == dt)
        ads = ads_all.where(F.col("dt") == dt)
        pd_ = player_daily(games, ads).cache()
        tables = {"player_daily": pd_, "harm_daily": harm_daily(pd_)}
        if labels_all is not None:
            tables["forced_deals_daily"] = forced_deals_daily(labels_all.where(F.col("dt") == dt))
        counts = {}
        for name, df in tables.items():
            df = df.withColumn("dt", F.lit(dt))
            df.coalesce(1).write.mode("overwrite").partitionBy("dt").parquet(f"{lake}/gold/{name}")
            counts[name] = df.count()
        pd_.unpersist()
        write_json(spark, f"{lake}/quality/silver_to_gold/dt={dt}", {"dt": dt, "rows": counts})
        print("ORO", dt, counts, flush=True)

    if args.catalog == "glue":
        for name in ("player_daily", "harm_daily", "forced_deals_daily"):
            register(spark, args.database, f"gold_{name}", f"{lake}/gold/{name}")
    spark.stop()


if __name__ == "__main__":
    main()

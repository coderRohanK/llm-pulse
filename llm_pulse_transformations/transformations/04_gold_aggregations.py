import dlt
from pyspark.sql.functions import (
    col, sum, avg, count, round,
    countDistinct, when, lit,
    window, current_timestamp,
    avg as spark_avg
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER = "llm_pulse_dev.silver"

# ─────────────────────────────────────────────────────────────
# TABLE 1: gold.daily_cost_by_team
# Used by: cost forecasting ML model + main dashboard chart
# ─────────────────────────────────────────────────────────────
@dlt.table(
    name    = "llm_pulse_dev.gold.daily_cost_by_team",
    comment = "Daily LLM spend per team with 7-day rolling average — gold layer",
    table_properties = {"layer": "gold"}
)
def daily_cost_by_team():
    df = spark.read.table(f"{SILVER}.api_calls")

    # Aggregate by team + date
    daily = (
        df.groupBy("event_date", "team")
        .agg(
            round(F.sum("cost_usd"), 4)          .alias("total_cost_usd"),
            F.count("*")                          .alias("total_calls"),
            round(F.avg("cost_usd"), 6)           .alias("avg_cost_per_call"),
            round(F.avg("latency_ms"), 0)         .alias("avg_latency_ms"),
            F.sum(F.when(col("is_error"), 1)
                   .otherwise(0))                 .alias("error_count"),
            F.sum(F.when(col("is_slow"), 1)
                   .otherwise(0))                 .alias("slow_call_count"),
            round(F.sum("total_tokens"), 0)       .alias("total_tokens_used"),
        )
    )

    # Add 7-day rolling average cost per team
    # This smooths out daily spikes and is used by the ML model as a feature
    window_7d = (
        Window
        .partitionBy("team")
        .orderBy(col("event_date").cast("long"))
        .rowsBetween(-6, 0)   # last 7 rows including current
    )

    result = (
        daily
        .withColumn("rolling_7d_avg_cost",
            round(F.avg("total_cost_usd").over(window_7d), 4))
        .withColumn("gold_loaded_at", current_timestamp())
        .orderBy("event_date", "team")
    )

    return result


# ─────────────────────────────────────────────────────────────
# TABLE 2: gold.model_performance_summary
# Used by: engineering dashboard view
# ─────────────────────────────────────────────────────────────
@dlt.table(
    name    = "llm_pulse_dev.gold.model_performance_summary",
    comment = "Daily model performance metrics — gold layer",
    table_properties = {"layer": "gold"}
)
def model_performance_summary():
    df = spark.read.table(f"{SILVER}.api_calls")

    return (
        df.groupBy("event_date", "model")
        .agg(
            F.count("*")                              .alias("total_calls"),
            round(F.avg("latency_ms"), 0)             .alias("avg_latency_ms"),
            round(F.sum("cost_usd"), 4)               .alias("total_cost_usd"),
            round(F.avg("cost_usd"), 6)               .alias("avg_cost_per_call"),
            round(F.sum("total_tokens"), 0)           .alias("total_tokens"),

            # Error rate as percentage
            round(
                F.sum(F.when(col("is_error"), 1).otherwise(0)) * 100.0
                / F.count("*"), 2
            ).alias("error_rate_pct"),

            # Slow call rate as percentage
            round(
                F.sum(F.when(col("is_slow"), 1).otherwise(0)) * 100.0
                / F.count("*"), 2
            ).alias("slow_call_rate_pct"),

            F.countDistinct("user_id")                .alias("unique_users"),
        )
        .withColumn("gold_loaded_at", current_timestamp())
        .orderBy("event_date", "model")
    )


# ─────────────────────────────────────────────────────────────
# TABLE 3: gold.feature_cost_breakdown
# Used by: finance dashboard — "which feature costs the most?"
# ─────────────────────────────────────────────────────────────
@dlt.table(
    name    = "llm_pulse_dev.gold.feature_cost_breakdown",
    comment = "Daily cost breakdown per product feature — gold layer",
    table_properties = {"layer": "gold"}
)
def feature_cost_breakdown():
    df = spark.read.table(f"{SILVER}.api_calls")

    return (
        df.groupBy("event_date", "team", "feature", "model")
        .agg(
            F.count("*")                          .alias("total_calls"),
            round(F.sum("cost_usd"), 4)           .alias("total_cost_usd"),
            round(F.avg("latency_ms"), 0)         .alias("avg_latency_ms"),
            round(F.sum("total_tokens"), 0)       .alias("total_tokens"),
            F.sum(F.when(col("is_error"), 1)
                   .otherwise(0))                 .alias("error_count"),
        )
        .withColumn("gold_loaded_at", current_timestamp())
        .orderBy("event_date", "total_cost_usd")
    )


# ─────────────────────────────────────────────────────────────
# TABLE 4: gold.quality_score_daily
# Used by: quality anomaly detection ML model (Day 7)
# ─────────────────────────────────────────────────────────────
@dlt.table(
    name    = "llm_pulse_dev.gold.quality_score_daily",
    comment = "Daily thumbs-up quality score per model — gold layer",
    table_properties = {"layer": "gold"}
)
def quality_score_daily():
    df = spark.read.table(f"{SILVER}.quality_feedback")

    daily = (
        df.groupBy("feedback_date", "model")
        .agg(
            F.count("*")                          .alias("total_feedback"),
            F.sum("thumbs_up")                    .alias("thumbs_up_count"),
            round(
                F.sum("thumbs_up") * 100.0
                / F.count("*"), 2
            ).alias("quality_score_pct"),   # e.g. 91.2 means 91.2% thumbs up
        )
        .withColumn("gold_loaded_at", current_timestamp())
        .orderBy("feedback_date", "model")
    )

    # Add 7-day rolling quality score — used by anomaly detection model
    window_7d = (
        Window
        .partitionBy("model")
        .orderBy(col("feedback_date").cast("long"))
        .rowsBetween(-6, 0)
    )

    return (
        daily
        .withColumn("rolling_7d_quality_score",
            round(F.avg("quality_score_pct").over(window_7d), 2))
    )
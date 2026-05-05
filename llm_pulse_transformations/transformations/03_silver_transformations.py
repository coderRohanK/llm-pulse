import dlt
from pyspark.sql.functions import (
    col, to_timestamp, round, when,
    lit, upper, trim, current_timestamp,
    coalesce, unix_timestamp
)
from pyspark.sql import functions as F

# ── Catalog references ─────────────────────────────────────
BRONZE = "llm_pulse_dev.bronze"
SILVER = "llm_pulse_dev.silver"

# ─────────────────────────────────────────────────────────────
# TABLE 1: silver.api_calls
# What we do here:
#   1. Combine historical + live calls into one unified table
#   2. Fix timestamp format (string → proper timestamp)
#   3. Add is_slow flag (latency > 3000ms = slow call)
#   4. Add total_tokens column
#   5. Join with pricing to calculate cost_usd per call
#   6. Apply data quality checks — drop bad records
# ─────────────────────────────────────────────────────────────

# Data quality rules — records failing these get DROPPED
@dlt.expect_or_drop("valid_call_id",       "call_id IS NOT NULL")
@dlt.expect_or_drop("valid_tokens",        "prompt_tokens > 0")
@dlt.expect_or_drop("valid_latency",       "latency_ms >= 0")
@dlt.expect_or_drop("valid_team",          "team IN ('product','data','marketing','ops','finance')")
@dlt.expect_or_drop("valid_model",         "model IN ('gpt-4o','gpt-3.5-turbo','claude-3-sonnet')")
@dlt.table(
    name    = "api_calls",
    comment = "Cleaned and cost-enriched LLM API calls — silver layer",
    table_properties = {"layer": "silver"}
)
def api_calls():

    # Step 1: Read historical calls from bronze
    historical = spark.read.table(f"{BRONZE}.api_calls_historical")

    # Step 2: Read live calls — union with historical
    # If api_calls_live is empty that is fine — union with nothing
    try:
        live = spark.read.table(f"{BRONZE}.api_calls_live")
        all_calls = historical.unionByName(live, allowMissingColumns=True)
    except Exception:
        all_calls = historical

    # Step 3: Read pricing table for cost calculation
    pricing = spark.read.table(f"{BRONZE}.model_pricing")

    # Step 4: Clean and enrich api calls
    cleaned = (
        all_calls
        # Fix timestamp — convert string to proper timestamp
        .withColumn("event_timestamp",
            to_timestamp(col("timestamp")))

        # Standardize text columns — trim whitespace, lowercase
        .withColumn("team",        trim(col("team")))
        .withColumn("model",       trim(col("model")))
        .withColumn("feature",     trim(col("feature")))
        .withColumn("environment", trim(col("environment")))
        .withColumn("status",      trim(col("status")))

        # Add total_tokens — sum of prompt + completion
        .withColumn("total_tokens",
            col("prompt_tokens") + col("completion_tokens"))

        # Add is_slow flag — calls taking over 3 seconds
        .withColumn("is_slow",
            when(col("latency_ms") > 3000, True).otherwise(False))

        # Add is_error flag — cleaner boolean version of status
        .withColumn("is_error",
            when(col("status") == "error", True).otherwise(False))

        # Extract date for partitioning and daily aggregations
        .withColumn("event_date",
            col("event_timestamp").cast("date"))

        # Drop original string timestamp — replaced by event_timestamp
        .drop("timestamp")

        # Add ingestion metadata
        .withColumn("silver_loaded_at", current_timestamp())
    )

    # Step 5: Join with pricing to calculate cost_usd
    # Formula: (prompt_tokens / 1000 * prompt_price)
    #        + (completion_tokens / 1000 * completion_price)
    enriched = (
        cleaned
        .join(pricing, on="model", how="left")
        .withColumn("cost_usd",
            round(
                (col("prompt_tokens")     / 1000 * col("prompt_price_per_1k_tokens")) +
                (col("completion_tokens") / 1000 * col("completion_price_per_1k_tokens")),
                6   # round to 6 decimal places — API costs are tiny per call
            )
        )
        # Drop pricing columns — keep only cost_usd
        .drop("prompt_price_per_1k_tokens",
              "completion_price_per_1k_tokens",
              "provider",
              "ingested_at")
    )

    return enriched


# ─────────────────────────────────────────────────────────────
# TABLE 2: silver.quality_feedback
# What we do here:
#   1. Fix date column type (string → date)
#   2. Add thumbs_up_bool as a proper boolean
#   3. Data quality check on model name
# ─────────────────────────────────────────────────────────────

@dlt.expect_or_drop("valid_feedback_model",
    "model IN ('gpt-4o','gpt-3.5-turbo','claude-3-sonnet')")
@dlt.expect_or_drop("valid_thumbs",
    "thumbs_up IN (0, 1)")
@dlt.table(
    name    = "quality_feedback",
    comment = "Cleaned quality feedback with proper types — silver layer",
    table_properties = {"layer": "silver"}
)
def quality_feedback():
    return (
        spark.read.table(f"{BRONZE}.quality_feedback")
        # Fix date type
        .withColumn("feedback_date",
            col("date").cast("date"))
        # Add boolean version of thumbs_up
        .withColumn("thumbs_up_bool",
            when(col("thumbs_up") == 1, True).otherwise(False))
        # Clean model column
        .withColumn("model", trim(col("model")))
        .drop("date", "ingested_at")
        .withColumn("silver_loaded_at", current_timestamp())
    )


# ─────────────────────────────────────────────────────────────
# TABLE 3: silver.model_pricing
# What we do here:
#   1. Validate pricing rows exist for all 3 models
#   2. Cast price columns to proper decimal type
# ─────────────────────────────────────────────────────────────

@dlt.expect_or_drop("valid_pricing_model",
    "model IN ('gpt-4o','gpt-3.5-turbo','claude-3-sonnet')")
@dlt.table(
    name    = "model_pricing",
    comment = "Validated model pricing reference table — silver layer",
    table_properties = {"layer": "silver"}
)
def model_pricing():
    return (
        spark.read.table(f"{BRONZE}.model_pricing")
        .withColumn("prompt_price_per_1k_tokens",
            col("prompt_price_per_1k_tokens").cast("double"))
        .withColumn("completion_price_per_1k_tokens",
            col("completion_price_per_1k_tokens").cast("double"))
        .withColumn("model", trim(col("model")))
        .drop("ingested_at")
        .withColumn("silver_loaded_at", current_timestamp())
    )
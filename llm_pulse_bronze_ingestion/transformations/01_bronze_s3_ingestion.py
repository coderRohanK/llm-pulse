import dlt
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType
)

# S3 path to your historical JSON files
S3_API_LOGS_PATH = "s3://llm-pulse-landing/api-logs/"
S3_PRICING_PATH  = "s3://llm-pulse-landing/pricing/"
S3_FEEDBACK_PATH = "s3://llm-pulse-landing/feedback/"

# ── Schema for API log events ──────────────────────────────
api_log_schema = StructType([
    StructField("call_id",           StringType(),  True),
    StructField("timestamp",         StringType(),  True),
    StructField("team",              StringType(),  True),
    StructField("feature",           StringType(),  True),
    StructField("model",             StringType(),  True),
    StructField("environment",       StringType(),  True),
    StructField("prompt_tokens",     IntegerType(), True),
    StructField("completion_tokens", IntegerType(), True),
    StructField("latency_ms",        IntegerType(), True),
    StructField("status",            StringType(),  True),
    StructField("status_code",       IntegerType(), True),
    StructField("user_id",           StringType(),  True),
])

# ── Table 1: Historical API call logs from S3 ──────────────
@dlt.table(
    name    = "api_calls_historical",
    comment = "Historical LLM API call logs ingested from S3 — bronze layer",
    table_properties = {"layer": "bronze"}
)
def api_calls_historical():
    return (
        spark.readStream
            .format("cloudFiles")           # Autoloader
            .option("cloudFiles.format", "json")
            .schema(api_log_schema)
            .load(S3_API_LOGS_PATH)
            .withColumn("ingested_at", current_timestamp())
    )

# ── Table 2: Model pricing reference ──────────────────────
@dlt.table(
    name    = "model_pricing",
    comment = "LLM model pricing per 1k tokens — bronze layer",
    table_properties = {"layer": "bronze"}
)
def model_pricing():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(S3_PRICING_PATH)
            .withColumn("ingested_at", current_timestamp())
    )

# ── Table 3: Quality feedback ──────────────────────────────
@dlt.table(
    name    = "quality_feedback",
    comment = "User thumbs up/down quality feedback per model — bronze layer",
    table_properties = {"layer": "bronze"}
)
def quality_feedback():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(S3_FEEDBACK_PATH)
            .withColumn("ingested_at", current_timestamp())
    )
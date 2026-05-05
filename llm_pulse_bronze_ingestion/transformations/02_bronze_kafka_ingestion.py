import dlt
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType
)

# Your laptop's IP where Kafka is running
# Find your laptop IP: run "ipconfig" on Windows or "ifconfig" on Mac
# Use the IPv4 address e.g. 192.168.1.100
KAFKA_BOOTSTRAP = "192.168.1.6:9092"  # CHANGE THIS to your laptop IP
KAFKA_TOPIC     = "llm-api-logs"

# Same schema as historical
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

# ── Table 4: Live API call events from Kafka ───────────────
@dlt.table(
    name    = "api_calls_live",
    comment = "Live LLM API call events streaming from Kafka — bronze layer",
    table_properties = {"layer": "bronze"}
)
def api_calls_live():
    raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
    )

    # Kafka delivers messages as raw bytes — parse JSON payload
    parsed = (
        raw
        .select(
            from_json(
                col("value").cast("string"),
                api_log_schema
            ).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        )
        .select("data.*", "kafka_timestamp")
        .withColumn("ingested_at", current_timestamp())
    )

    return parsed
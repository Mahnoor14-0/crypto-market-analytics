from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    LongType
)


# ============================================================
# BRONZE SCHEMA
# ============================================================

BRONZE_COIN_MARKET_RAW_SCHEMA = StructType([
    StructField("record_hash", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("source", StringType(), False),
    StructField("source_endpoint", StringType(), False),
    StructField("coin_id", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("raw_response", StringType(), False),
    StructField("load_timestamp", TimestampType(), False)
])


# ============================================================
# SILVER SCHEMA
# ============================================================

SILVER_MARKET_OBSERVATION_SCHEMA = StructType([
    StructField("coin_id", StringType(), False),
    StructField("observation_timestamp", TimestampType(), False),
    StructField("price_usd", DoubleType(), False),
    StructField("market_cap_usd", DoubleType(), False),
    StructField("total_volume_usd", DoubleType(), False),
    StructField("load_timestamp", TimestampType(), False),
    StructField("load_type", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("run_id", StringType(), False)
])


# ============================================================
# QUARANTINE SCHEMA
# ============================================================

SILVER_QUARANTINE_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("source", StringType(), False),
    StructField("coin_id", StringType(), True),
    StructField("raw_record", StringType(), False),
    StructField("error_reason", StringType(), False),
    StructField("load_timestamp", TimestampType(), False)
])


# ============================================================
# PIPELINE LOG SCHEMA
# ============================================================

PIPELINE_EXECUTION_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("parameter", StringType(), True),
    StructField("file_processed", StringType(), False),
    StructField("execution_start", TimestampType(), False),
    StructField("execution_end", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("rows_inserted", LongType(), False),
    StructField("rows_updated", LongType(), False),
    StructField("error_message", StringType(), True)
])
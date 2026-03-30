"""
Silver Layer - CDC Cleansing, Deduplication & Validation
----------------------------------------------------------
Medallion Architecture | CDC Pipeline
Layer   : Silver
Purpose : Read Bronze CDC records, apply data quality rules, deduplicate events,
          handle late-arriving records, and produce a clean, validated dataset
          ready for SCD2 processing in the Gold layer.

Input   : Delta table >> bronze.customer_cdc_raw
Output  : Delta table >> silver.customer_cdc_clean
Checkpoint: /mnt/checkpoints/silver/customer_cdc/
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BRONZE_TABLE      = "bronze.customer_cdc_raw"
SILVER_TABLE      = "silver.customer_cdc_clean"
SILVER_TABLE_PATH = "/mnt/delta/silver/customer_cdc_clean"
CHECKPOINT_PATH   = "/mnt/checkpoints/silver/customer_cdc/"
SILVER_DATABASE   = "silver"

VALID_OPERATIONS  = {"INSERT", "UPDATE"}


# ---------------------------------------------------------------------------
# Step 1 - Type casting & timestamp parsing
# ---------------------------------------------------------------------------
def cast_types(df: DataFrame) -> DataFrame:
    """
    Convert raw string columns to their correct types.

    - update_ts  : cast from STRING to TIMESTAMP
                   Format: "yyyy-MM-dd'T'HH:mm:ss"
    - operation  : trim whitespace and upper-case for consistent comparisons
    """
    return df.withColumn(
        "update_ts", F.to_timestamp(F.col("update_ts"), "yyyy-MM-dd'T'HH:mm:ss")
    ).withColumn(
        "operation", F.upper(F.trim(F.col("operation")))
    )


# ---------------------------------------------------------------------------
# Step 2 - Null / mandatory field validation
# ---------------------------------------------------------------------------
def validate_not_null(df: DataFrame) -> DataFrame:
    """
    Drop records where customer_id is NULL.
    These records cannot be processed or joined downstream.
    Rejected rows are logged for observability.
    """
    valid   = df.filter(F.col("customer_id").isNotNull())
    invalid = df.filter(F.col("customer_id").isNull())

    invalid_count = invalid.count()
    if invalid_count > 0:
        logger.warning("Dropping %d records with NULL customer_id", invalid_count)
        # In production: write invalid rows to a quarantine/dead-letter table
        invalid.write.format("delta").mode("append").save("/mnt/delta/quarantine/customer_null_id")

    return valid


# ---------------------------------------------------------------------------
# Step 3 - Operation validation
# ---------------------------------------------------------------------------
def validate_operations(df: DataFrame) -> DataFrame:
    """
    Retain only records whose operation value is in VALID_OPERATIONS.
    Unknown operations (e.g. DELETE, UPSERT) are rejected.
    """
    valid_ops_list = list(VALID_OPERATIONS)
    valid   = df.filter(F.col("operation").isin(valid_ops_list))
    invalid = df.filter(~F.col("operation").isin(valid_ops_list))

    invalid_count = invalid.count()
    if invalid_count > 0:
        logger.warning(
            "Dropping %d records with invalid operation values", invalid_count
        )

    return valid


# ---------------------------------------------------------------------------
# Step 4 - Deduplication
# ---------------------------------------------------------------------------
def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove exact duplicate CDC events.
    A duplicate is defined as two rows sharing the same (customer_id, update_ts).

    Strategy: keep the first occurrence ordered by bronze_ingestion_ts
    (the row that arrived earliest in Bronze).

    This also handles the case where the source system re-emits an event
    on retry — both copies will have the same update_ts so only one survives.
    """
    dedup_window = Window.partitionBy("customer_id", "update_ts").orderBy(F.col("bronze_ingestion_ts").asc())

    return (
        df
        .withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# Step 5 - Late-arriving event handling
# ---------------------------------------------------------------------------
def handle_late_arrivals(df: DataFrame) -> DataFrame:
    """
    Late-arriving events are events whose update_ts is OLDER than records already seen for that customer.

    Silver's responsibility: ensure the events are correctly ORDERED by update_ts per customer before the Gold SCD2 merge reads them.
    The Gold layer will then recompute affected history windows.

    We add a silver_processing_ts column so the Gold layer can distinguish
    true event order (update_ts) from processing order (silver_processing_ts).

    Watermark: In production streaming mode, apply a watermark to bound
    late data. Here we configure a 7-day tolerance.
    """
    return df.withColumn(
        "silver_processing_ts", F.current_timestamp()
    ).withWatermark(
        "update_ts", "7 days"       # tolerate events up to 7 days late
    )


# ---------------------------------------------------------------------------
# Step 6 - Add Silver audit column
# ---------------------------------------------------------------------------
def add_silver_metadata(df: DataFrame) -> DataFrame:
    """Tag each record with the Silver pipeline run date for lineage."""
    return df.withColumn(
        "silver_loaded_date", F.current_date()
    )


# ---------------------------------------------------------------------------
# Orchestrate all transformation steps
# ---------------------------------------------------------------------------
def transform(df: DataFrame) -> DataFrame:
    """
    Chain all Silver transformation steps in order:
    cast >> validate nulls >> validate ops >> deduplicate >> late arrivals >> metadata
    """
    df = cast_types(df)
    df = validate_not_null(df)
    df = validate_operations(df)
    df = deduplicate(df)
    df = handle_late_arrivals(df)
    df = add_silver_metadata(df)
    return df


# ---------------------------------------------------------------------------
# Database / table registration
# ---------------------------------------------------------------------------
def create_silver_database() -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}")
    logger.info("Silver database ready: %s", SILVER_DATABASE)


def register_table() -> None:
    spark.sql(f""" 
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
        USING DELTA
        LOCATION '{SILVER_TABLE_PATH}'
    """)
    logger.info("Silver table registered: %s", SILVER_TABLE)


# ---------------------------------------------------------------------------
# Streaming pipeline wiring
# ---------------------------------------------------------------------------
def read_bronze_stream():
    """Read Bronze Delta table as a stream (only new rows since last checkpoint)."""
    return (
        spark.readStream
        .format("delta")
        .table(BRONZE_TABLE)
    )


def write_silver_stream(stream_df):
    """
    Write clean records to Silver Delta using foreachBatch.

    foreachBatch gives us:
    - Full DataFrame API (needed for window-based deduplication)
    - Ability to run arbitrary Delta merge / quality checks
    """

    def process_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        logger.info("Silver - processing micro-batch id=%d, rows=%d", batch_id, batch_df.count())

        clean_df = transform(batch_df)

        (
            clean_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(SILVER_TABLE_PATH)
        )
        logger.info("Silver - micro-batch %d written successfully", batch_id)

    return (
        stream_df.writeStream
        .format("delta")
        .foreachBatch(process_micro_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)   # process all available data then stop (batch-style)
        .start()
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run_silver_pipeline(await_termination: bool = True) -> None:
    create_silver_database()
    register_table()

    bronze_stream = read_bronze_stream()
    query         = write_silver_stream(bronze_stream)

    logger.info("Silver pipeline running. Awaiting termination: %s", await_termination)

    if await_termination:
        query.awaitTermination()


if __name__ == "__main__":
    run_silver_pipeline(await_termination=True)

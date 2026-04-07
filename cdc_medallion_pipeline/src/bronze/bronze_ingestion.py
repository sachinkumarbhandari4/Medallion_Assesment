"""
Bronze Layer – Raw CDC Ingestion using Auto Loader
Medallion Architecture | CDC Pipeline
Layer   : Bronze
Purpose : Ingest raw JSON CDC events from cloud storage using Databricks Auto Loader.
          Records are stored without transformation. Ingestion timestamp is added.
          Schema evolution is enabled so new fields in source do not break the pipeline.

Input   : /mnt/raw/customer_updates/     (JSON files, near real-time)
Output  : Delta table  →  bronze.customer_cdc_raw
Checkpoint: /mnt/checkpoints/bronze/customer_cdc/
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration – centralized so it is easy to override per environment
# ---------------------------------------------------------------------------
RAW_SOURCE_PATH       = "/mnt/raw/customer_updates/"
BRONZE_TABLE_PATH     = "/mnt/delta/bronze/customer_cdc_raw"
CHECKPOINT_PATH       = "/mnt/checkpoints/bronze/customer_cdc/"
BRONZE_DATABASE       = "bronze"
BRONZE_TABLE_NAME     = "customer_cdc_raw"
SCHEMA_HINTS_PATH     = "/mnt/schema_hints/customer_cdc/"   # Auto Loader schema inference store

# ---------------------------------------------------------------------------
# Explicit schema hint.
# Auto Loader will still handle schema evolution via mergeSchema = true.
# ---------------------------------------------------------------------------
CUSTOMER_CDC_SCHEMA = StructType([
    StructField("customer_id", StringType(),   nullable=True),
    StructField("name",        StringType(),   nullable=True),
    StructField("email",       StringType(),   nullable=True),
    StructField("city",        StringType(),   nullable=True),
    StructField("status",      StringType(),   nullable=True),
    StructField("operation",   StringType(),   nullable=True),
    StructField("update_ts",   StringType(),   nullable=True),
])


def create_bronze_database() -> None:
    """Ensure the Bronze database/schema exists."""
    try:  # exception handling: catch errors during database creation
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}")
        logger.info("Bronze database ready: %s", BRONZE_DATABASE)
    except Exception as e:  # exception handling: log and re-raise so caller is aware
        logger.error("Failed to create Bronze database '%s': %s", BRONZE_DATABASE, e)
        raise


def read_stream_auto_loader():
    """
    Configure Auto Loader to read JSON files incrementally.

    Key options:
    - cloudFiles.format         : source file format
    - cloudFiles.schemaLocation : persists inferred schema so restarts are fast
    - mergeSchema               : absorbs new columns without failing the stream
    - cloudFiles.inferColumnTypes: keeps inferred types stable across micro-batches
    """
    logger.info("Configuring Auto Loader stream from: %s", RAW_SOURCE_PATH)

    try:
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", SCHEMA_HINTS_PATH)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("mergeSchema", "true")
            .schema(CUSTOMER_CDC_SCHEMA) # Providing a hint schema so Auto Loader starts immediately on first run without scanning all files to infer types.
            .load(RAW_SOURCE_PATH)
        )
    except Exception as e:
        logger.error("Failed to configure Auto Loader stream from '%s': %s", RAW_SOURCE_PATH, e)
        raise


def add_audit_columns(df):
    """
    Every raw record with pipeline audit metadata.
    """
    try:
        return df.withColumn(
            "bronze_ingestion_ts", F.current_timestamp()
        ).withColumn(
            "source_file_name", F.input_file_name()
        )
    except Exception as e:
        logger.error("Failed to add audit columns: %s", e)
        raise


def write_stream_to_bronze(stream_df):
    """
    Append records to the Bronze Delta table using a Structured Streaming sink.

    - OutputMode : append
    - checkpointLocation : guarantees exactly-once delivery on restart
    - mergeSchema : forward-compatible with schema changes from Auto Loader
    """
    logger.info("Starting write stream to Bronze table: %s.%s", BRONZE_DATABASE, BRONZE_TABLE_NAME)

    try:
        return (
            stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .option("mergeSchema", "true")
            .partitionBy("operation")
            .toTable(f"{BRONZE_DATABASE}.{BRONZE_TABLE_NAME}")
        )
    except Exception as e:
        logger.error(
            "Failed to start write stream to '%s.%s' with checkpoint '%s': %s",
            BRONZE_DATABASE, BRONZE_TABLE_NAME, CHECKPOINT_PATH, e
        )
        raise


def register_table() -> None:
    """
    Register the Bronze Delta table in the metastore so it is queryable via SQL.
    """
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_DATABASE}.{BRONZE_TABLE_NAME}
            USING DELTA
            LOCATION '{BRONZE_TABLE_PATH}'
        """)
        logger.info("Bronze table registered: %s.%s", BRONZE_DATABASE, BRONZE_TABLE_NAME)
    except Exception as e:
        logger.error(
            "Failed to register Bronze table '%s.%s' at '%s': %s",
            BRONZE_DATABASE, BRONZE_TABLE_NAME, BRONZE_TABLE_PATH, e
        )
        raise


def run_bronze_pipeline(await_termination: bool = True) -> None:
    """
    Entry point for the Bronze ingestion pipeline.

    Parameters
    ----------
    await_termination : bool
        Set True for production (blocks until stream stops).
        Set False for notebook interactive mode.
    """
    try:
        create_bronze_database()
        register_table()

        raw_stream = read_stream_auto_loader()
        enriched   = add_audit_columns(raw_stream)
        query      = write_stream_to_bronze(enriched)

        logger.info("Bronze pipeline running. Awaiting termination: %s", await_termination)

        if await_termination:
            query.awaitTermination()

    except Exception as e:
        logger.error("Bronze pipeline terminated with an unexpected error: %s", e)
        raise


# ---------------------------------------------------------------------------
# Script entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_bronze_pipeline(await_termination=True)

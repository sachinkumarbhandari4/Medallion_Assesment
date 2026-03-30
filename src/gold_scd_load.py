"""
Gold Layer – SCD Type 2 Customer Dimension
-----------------------------------------
Medallion Architecture | CDC Pipeline
Layer   : Gold
Purpose : Merge clean CDC events from Silver into a Slowly Changing Dimension
          Type 2 (SCD2) Customer Dimension table that maintains full history.

Input   : Delta table  >> silver.customer_cdc_clean
Output  : Delta table  >> gold.dim_customer
Checkpoint: /mnt/checkpoints/gold/dim_customer/

SCD2 Rules
----------
New customer (INSERT, no existing row):
    >> Insert row: start_date = update_ts, end_date = NULL, is_current = TRUE

Existing customer (UPDATE, attributes changed):
    1. Close current row : end_date = update_ts, is_current = FALSE
    2. Insert new row    : start_date = update_ts, end_date = NULL, is_current = TRUE

Out-of-order events:
    Events are sorted by update_ts per customer before applying SCD2 so that
    a late-arriving older event does not incorrectly close a newer row.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SILVER_TABLE       = "silver.customer_cdc_clean"
GOLD_TABLE         = "gold.dim_customer"
GOLD_TABLE_PATH    = "/mnt/delta/gold/dim_customer"
CHECKPOINT_PATH    = "/mnt/checkpoints/gold/dim_customer/"
GOLD_DATABASE      = "gold"

# Columns that, when changed, trigger a new SCD2 version
SCD2_TRACKED_COLS  = ["city", "status", "email", "name"]


# ---------------------------------------------------------------------------
# Table initialisation
# ---------------------------------------------------------------------------
def create_gold_database() -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}") 
    logger.info("Gold database ready: %s", GOLD_DATABASE)


def initialise_gold_table() -> None:
    """
    Create the Gold dim_customer Delta table if it does not already exist.
    customer_sk is a monotonically increasing surrogate key generated at insert time.
    """
    spark.sql(f""" 
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            customer_sk  BIGINT      GENERATED ALWAYS AS IDENTITY,
            customer_id  STRING      NOT NULL,
            name         STRING,
            email        STRING,
            city         STRING,
            status       STRING,
            start_date   TIMESTAMP   NOT NULL,
            end_date     TIMESTAMP,
            is_current   BOOLEAN     NOT NULL
        )
        USING DELTA
        LOCATION '{GOLD_TABLE_PATH}'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    logger.info("Gold table initialised: %s", GOLD_TABLE)


# ---------------------------------------------------------------------------
# SCD2 – set-based MERGE (production approach)
# ---------------------------------------------------------------------------
def apply_scd2_set_based(batch_df: DataFrame, batch_id: int) -> None:
    """
    Pure SQL set-based SCD2 merge – more scalable than row-by-row for large batches.

    Strategy:
    1.  Rank incoming events per customer by update_ts (latest wins for current record).
    2.  Join against Gold to identify rows that need closing.
    3.  Close stale rows in one MERGE pass.
    4.  Insert all new versions in one INSERT pass.

    Use this method when batch sizes exceed ~100k distinct customers per micro-batch.
    """
    gold_dt = DeltaTable.forPath(spark, GOLD_TABLE_PATH) 

    # Rank incoming events: rank=1 is the LATEST event per customer in this batch
    rank_window = Window.partitionBy("customer_id").orderBy(F.col("update_ts").desc())

    latest_events = (
        batch_df
        .withColumn("_rank", F.rank().over(rank_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    # Step 1: Close current Gold records that are outdated by incoming events
    (
        gold_dt.alias("target")
        .merge(
            latest_events.alias("source"),
            condition=(
                "target.customer_id = source.customer_id "
                "AND target.is_current = true "
                "AND target.start_date < source.update_ts"
            )
        )
        .whenMatchedUpdate(set={
            "end_date":   "source.update_ts",
            "is_current": "false"
        })
        .execute()
    )

    # Step 2: Insert all new versions from this batch
    (
        latest_events
        .select(
            F.col("customer_id"),
            F.col("name"),
            F.col("email"),
            F.col("city"),
            F.col("status"),
            F.col("update_ts").alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(True).alias("is_current")
        )
        .write
        .format("delta")
        .mode("append")
        .save(GOLD_TABLE_PATH)
    )

    logger.info("Gold SCD2 set-based – batch %d complete", batch_id)


# ---------------------------------------------------------------------------
# Streaming pipeline wiring
# ---------------------------------------------------------------------------
def read_silver_stream():
    """Read Silver Delta table as a stream."""
    return (
        spark.readStream
        .format("delta")
        .table(SILVER_TABLE)
    )


def write_gold_stream(stream_df):
    """Wire the Silver stream into the SCD2 foreachBatch processor."""

    def micro_batch_handler(batch_df: DataFrame, batch_id: int) -> None:
        apply_scd2_set_based(batch_df, batch_id)

    return (
        stream_df.writeStream
        .format("delta")
        .foreachBatch(micro_batch_handler)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run_gold_pipeline(await_termination: bool = True) -> None:
    create_gold_database()
    initialise_gold_table()

    silver_stream = read_silver_stream()
    query         = write_gold_stream(silver_stream)

    logger.info("Gold SCD2 pipeline running. Awaiting termination: %s", await_termination)

    if await_termination:
        query.awaitTermination()


if __name__ == "__main__":
    run_gold_pipeline(await_termination=True)

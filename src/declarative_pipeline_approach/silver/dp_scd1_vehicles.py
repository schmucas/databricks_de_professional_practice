"""
Silver | dp_scd1_vehicles | declarative track

Mirrors : src/classic_approach/silver/batch/silver_scd1_vehicles.ipynb
Source  : bronze.vehicles_raw  (slow-moving master data, Auto Loader)
Target  : dp_scd1_vehicles  (SCD Type 1, current state only)

Parity notes
  - filters vehicle_id IS NULL
  - dedupes to the latest row per vehicle_id, ordered by last_updated (descending)
  - casts: capacity_kg INT, cold_chain_capable BOOLEAN,
           commissioned_date DATE, last_updated TIMESTAMP
  - column list: capacity_kg, cold_chain_capable, commissioned_date, home_depot,
                 last_updated, model, plate_number, vehicle_id, vehicle_type
  - no history, no deletes, no backfill

The classic notebook hand-writes the dedupe window plus a MERGE.
Here the dedupe collapses into the auto CDC flow (keys + sequence_by).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, IntegerType

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

# bronze is not the pipeline default schema, so it is qualified; silver is, so
# the target is named bare.
SOURCE_TABLE = f"{CATALOG}.bronze.vehicles_raw"
TARGET_TABLE = "dp_scd1_vehicles"

SOURCE_VIEW = "dp_vehicles_changes"


# --- source view ------------------------------------------------------------
@dp.temporary_view(name=SOURCE_VIEW)
def dp_vehicles_changes():
    """Cast the bronze fleet-master feed to the classic silver column shape.

    Returns:
        Streaming DataFrame carrying the nine classic business columns plus the
        `_insert_update_ts` bookkeeping timestamp, one row per arriving
        full-row post-image.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("capacity_kg", F.col("capacity_kg").cast(IntegerType()))
        .withColumn("cold_chain_capable", F.col("cold_chain_capable").cast(BooleanType()))
        .withColumn("commissioned_date", F.col("commissioned_date").cast(DateType()))
        .withColumn("last_updated", F.to_timestamp(F.col("last_updated")))
        .withColumn("_insert_update_ts", F.current_timestamp())
        .select(
            "capacity_kg",
            "cold_chain_capable",
            "commissioned_date",
            "home_depot",
            "last_updated",
            "model",
            "plate_number",
            "vehicle_id",
            "vehicle_type",
            "_insert_update_ts",
        )
    )


# --- target -----------------------------------------------------------------
dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Fleet master, SCD Type 1: latest full-row post-image per vehicle_id.",
    cluster_by=["_insert_update_ts", "vehicle_id"],
    expect_all_or_drop={"valid_vehicle_id": "vehicle_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target=TARGET_TABLE,
    source=SOURCE_VIEW,
    keys=["vehicle_id"],
    sequence_by=F.col("last_updated"),
    stored_as_scd_type=1,
)


# Paradigm note
#   Classic needs two moving parts for "latest wins": a row_number window inside
#   the batch, then a MERGE to reconcile that batch against history. Auto CDC does
#   both from one sequence_by, and it stays correct across batches, where the
#   classic window only ever sees the rows in front of it.
#   The classic NULL filter becomes an expectation, so the drop count is now a
#   published pipeline metric instead of rows silently vanishing in a .filter().

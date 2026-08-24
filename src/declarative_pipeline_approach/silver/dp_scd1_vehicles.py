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

# --- config -----------------------------------------------------------------
SOURCE_TABLE = "bronze.vehicles_raw"
TARGET_TABLE = "dp_scd1_vehicles"


# --- source view ------------------------------------------------------------
# TODO: temporary view over the bronze stream, with the classic casts and filter.


# --- target -----------------------------------------------------------------
# TODO: create_streaming_table(TARGET_TABLE, ...)
# TODO: auto CDC flow, keys=["vehicle_id"], sequence_by=last_updated, stored_as_scd_type=1

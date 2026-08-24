"""
Silver | dp_shipment_events | declarative track

Mirrors : src/classic_approach/silver/batch/silver_shipment_events.ipynb
Source  : bronze.shipment_events_raw  (high-volume event stream, Auto Loader)
Target  : dp_shipment_events  (append-only, deduplicated)

Parity notes
  - filters event_id IS NULL
  - roughly 3 percent duplicate event_ids, keep the earliest by event_timestamp
    (the classic notebook dedupes ascending on event_id / event_timestamp)
  - casts: event_timestamp TIMESTAMP, latitude DOUBLE, longitude DOUBLE,
           temperature_celsius DOUBLE
  - derived: event_date = to_date(event_timestamp)
  - column list: event_id, event_timestamp, event_type, latitude, longitude,
                 order_id, shipment_status, source_system, temperature_celsius,
                 vehicle_id, event_date

Dedupe on a streaming table is the interesting bit: an auto CDC flow with
stored_as_scd_type=1 gives idempotent last-writer-wins on event_id without
a stateful dropDuplicates.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
SOURCE_TABLE = "bronze.shipment_events_raw"
TARGET_TABLE = "dp_shipment_events"


# --- source view ------------------------------------------------------------
# TODO: temporary view over the bronze stream, with the classic casts and filter.


# --- target -----------------------------------------------------------------
# TODO: create_streaming_table(TARGET_TABLE, ...) with expectations
# TODO: dedupe strategy on event_id

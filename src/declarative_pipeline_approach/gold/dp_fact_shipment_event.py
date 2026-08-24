"""
Gold | dp_fact_shipment_event | declarative track

Mirrors : src/classic_approach/gold/gold_fact_shipment_event.ipynb
Sources : dp_shipment_events (silver), dp_dim_vehicle, dp_dim_date
Target  : dp_fact_shipment_event
Grain   : one row per tracking event  (PK event_id)

Parity notes
  - joins dim_vehicle on vehicle_id and dim_date on event_date, both LEFT
  - order_id rides along as a degenerate dimension, there is no order FK here
  - every other silver column passes through unchanged
  - drops the silver bookkeeping timestamp column

The append-only fact: silver is already deduplicated and never updates,
so this can be a streaming table rather than a materialized view. Worth
being deliberate about that choice, an interviewer will ask why.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
SOURCE_EVENTS = "dp_shipment_events"
DIM_VEHICLE = "dp_dim_vehicle"
DIM_DATE = "dp_dim_date"
TARGET_TABLE = "dp_fact_shipment_event"


# --- target -----------------------------------------------------------------
# TODO: table named TARGET_TABLE, streaming or materialized, decide and note why

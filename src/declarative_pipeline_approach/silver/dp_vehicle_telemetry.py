"""
Silver | dp_vehicle_telemetry | declarative track

Mirrors : src/classic_approach/silver/batch/silver_vehicle_telemetry.ipynb
          (plus silver_vehicle_telemetry_transforms.py)
Source  : bronze.vehicle_telemetry_raw  (IoT, at-least-once delivery, Auto Loader)
Target  : dp_vehicle_telemetry  (append-only, deduplicated, enriched)

Parity notes, normalize step
  - filters reading_id IS NULL, dedupe key is reading_id (timestamps are NOT unique)
  - casts: reading_timestamp TIMESTAMP, cargo_temp_c DOUBLE, engine_temp_c DOUBLE,
           fuel_pct DOUBLE, odometer_km INT, speed_kmh DOUBLE
  - out-of-range sensor values become NULL:
        cargo_temp_c  outside [-273, 100]
        engine_temp_c outside [0, 200]
        speed_kmh     outside [0, 180]

Parity notes, enrich step
  - is_cold_chain_cargo = cargo_temp_c IS NOT NULL
  - speed_limit_exceed  = speed_kmh > 120
  - engine_overheat     = engine_temp_c > 110
  - reading_date        = to_date(reading_timestamp)

The classic track puts the range checks in Python and keeps the row.
The declarative version should express them as expectations, so the same
rules also show up as pipeline data-quality metrics.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
SOURCE_TABLE = "bronze.vehicle_telemetry_raw"
TARGET_TABLE = "dp_vehicle_telemetry"


# --- source view ------------------------------------------------------------
# TODO: temporary view, normalize then enrich (mirror the two transform functions).


# --- target -----------------------------------------------------------------
# TODO: create_streaming_table(TARGET_TABLE, ...) with expectations
# TODO: dedupe strategy on reading_id

"""
Gold | dp_dim_vehicle | declarative track

Mirrors : src/classic_approach/gold/gold_dim_vehicle.ipynb
Source  : dp_scd1_vehicles  (silver)
Target  : dp_dim_vehicle  (SCD Type 1 dimension)

Parity notes
  - vehicle_sk = surrogate key hashed from (vehicle_id)
  - current state only, no start_at / end_at / is_current
  - business columns: vehicle_id, plate_number, model, vehicle_type, capacity_kg,
                      cold_chain_capable, home_depot, commissioned_date

The thinnest table in the track: silver already holds current state, so gold
adds a surrogate key and nothing else.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
SOURCE_TABLE = "dp_scd1_vehicles"
TARGET_TABLE = "dp_dim_vehicle"


# --- target -----------------------------------------------------------------
# TODO: materialized view named TARGET_TABLE
# TODO: surrogate key on (vehicle_id), match the classic hash exactly

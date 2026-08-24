"""
Gold | dp_dim_location | declarative track

Mirrors : src/classic_approach/gold/gold_dim_location.ipynb
Sources : dp_scd2_orders (silver), dp_static_location_lookup (silver)
Target  : dp_dim_location

Parity notes
  - collects distinct cities from BOTH order columns (origin_city, destination_city),
    dropping nulls and empty strings, then unions and dedupes them
  - joins the lookup on a normalized city string (the classic add_normalized_str_col
    helper), not on the raw value
  - cities with no lookup match become 'unknown' rather than being dropped
  - broadcast the lookup, it is 26 rows
  - location_sk = surrogate key hashed from (city)
  - columns: location_sk, city, realm_name, realm_code, region, language_region

Careful: the classic notebook had a bug here once, reading a stale lookup
table name. Point the source at dp_static_location_lookup.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
SOURCE_ORDERS = "dp_scd2_orders"
SOURCE_LOOKUP = "dp_static_location_lookup"
TARGET_TABLE = "dp_dim_location"


# --- target -----------------------------------------------------------------
# TODO: materialized view named TARGET_TABLE
# TODO: surrogate key on (city), match the classic hash exactly

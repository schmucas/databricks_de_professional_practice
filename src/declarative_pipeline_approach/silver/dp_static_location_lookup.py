"""
Silver | dp_static_location_lookup | declarative track

Mirrors : src/classic_approach/silver/static/silver_static_location_lookup.ipynb
Source  : hardcoded seed list (no upstream system)
Target  : dp_static_location_lookup  (small reference table)

Parity notes
  - 26 Middle-earth cities
  - columns: city, realm_name, realm_code, region, language_region
  - values are identical to the classic notebook, copy the list verbatim
  - static reference data, so a materialized view is the right shape here,
    not a streaming table

This is the only silver table with no bronze parent. It exists so
dp_dim_location can enrich the raw city strings coming off orders.
"""

from pyspark import pipelines as dp
from pyspark.sql.types import StringType, StructField, StructType

# --- config -----------------------------------------------------------------
TARGET_TABLE = "dp_static_location_lookup"


# --- seed data --------------------------------------------------------------
# TODO: copy the city list and schema verbatim from the classic notebook.


# --- target -----------------------------------------------------------------
# TODO: materialized view named TARGET_TABLE

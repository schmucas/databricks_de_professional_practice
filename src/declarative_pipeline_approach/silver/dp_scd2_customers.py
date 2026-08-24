"""
Silver | dp_scd2_customers | declarative track

Mirrors : src/classic_approach/silver/stream/silver_scd2_customers.ipynb
Source  : bronze.customers_raw  (Delta Change Data Feed, full-row post-images)
Target  : dp_scd2_customers  (SCD Type 2)

Pattern
  bronze CDF -> streaming source view -> create_streaming_table -> auto CDC flow (SCD 2)

Parity notes (what the classic foreachBatch MERGE does by hand)
  - drops _change_type = 'update_preimage'
  - orders changes inside a batch by _commit_version
  - closes the previous current row and opens a new one (start_at / end_at / is_current)
  - source deletes are flagged so gold can erase downstream (right-to-erasure path)

Declarative equivalents to reach for
  - sequence_by      -> the classic ordering key (_commit_version)
  - apply_as_deletes -> _change_type = 'delete'
  - except_column_list -> CDF metadata columns
  - expectations     -> customer_id NOT NULL
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
# TODO: read pipeline configuration for env / catalog / schema, do not hardcode.
SOURCE_TABLE = "bronze.customers_raw"
TARGET_TABLE = "dp_scd2_customers"


# --- source view ------------------------------------------------------------
# TODO: temporary view over the bronze CDF stream, with the classic casts
#       (postal_code INT, signup_date DATE, last_updated TIMESTAMP).


# --- target -----------------------------------------------------------------
# TODO: create_streaming_table(TARGET_TABLE, ...)
# TODO: auto CDC flow, keys=["customer_id"], stored_as_scd_type=2

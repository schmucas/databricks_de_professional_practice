"""
Silver | dp_scd2_orders | declarative track

Mirrors : src/classic_approach/silver/stream/silver_scd2_orders.ipynb
Source  : bronze.orders_raw  (file-based CDC export, Auto Loader, full-row post-images)
Target  : dp_scd2_orders  (SCD Type 2, order-status history)

Pattern
  bronze stream -> streaming source view -> create_streaming_table -> auto CDC flow (SCD 2)

Parity notes
  - filters order_id IS NULL
  - sequences by _change_ts (not a Delta commit version, this source is file-based)
  - no deletes on this source
  - casts: customer_id INT, quantity INT, total_amount DOUBLE,
           order_date / delivery_date DATE, _change_ts TIMESTAMP
  - ~2 percent of rows arrive with a missing total_amount, keep them, do not drop

Contrast worth keeping visible: same SCD2 outcome as dp_scd2_customers,
different change-capture mechanism (CDF vs file-based CDC).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
SOURCE_TABLE = "bronze.orders_raw"
TARGET_TABLE = "dp_scd2_orders"


# --- source view ------------------------------------------------------------
# TODO: temporary view over the bronze stream, with the classic casts and filter.


# --- target -----------------------------------------------------------------
# TODO: create_streaming_table(TARGET_TABLE, ...)
# TODO: auto CDC flow, keys=["order_id"], sequence_by=_change_ts, stored_as_scd_type=2

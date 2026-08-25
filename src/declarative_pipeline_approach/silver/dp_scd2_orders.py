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

That difference shows up in the sequencing. Customers needs a struct to separate
"what orders the changes" from "what stamps the validity window"; orders does not,
because _change_ts already does both jobs in the classic notebook. So __START_AT
here is a plain timestamp and equals the classic start_at exactly.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, TimestampType

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

# bronze is not the pipeline default schema, so it is qualified; silver is, so
# the target is named bare.
SOURCE_TABLE = f"{CATALOG}.bronze.orders_raw"
TARGET_TABLE = "dp_scd2_orders"

SOURCE_VIEW = "dp_orders_changes"


# --- source view ------------------------------------------------------------
@dp.temporary_view(name=SOURCE_VIEW)
def dp_orders_changes():
    """Cast the bronze order change feed to the classic silver column shape.

    Returns:
        Streaming DataFrame of order change events, one row per full-row
        post-image, carrying the twelve classic business columns plus
        `_insert_update_ts` and the `_change_ts` sequence column.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("_change_ts", F.col("_change_ts").cast(TimestampType()))
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
        .withColumn("delivery_date", F.col("delivery_date").cast(DateType()))
        .withColumn("order_date", F.col("order_date").cast(DateType()))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("_insert_update_ts", F.current_timestamp())
        .select(
            "order_id",
            "customer_id",
            "delivery_date",
            "destination_city",
            "order_date",
            "origin_city",
            "payment_method",
            "product_code",
            "product_name",
            "quantity",
            "status",
            "total_amount",
            "_insert_update_ts",
            "_change_ts",
        )
    )


# --- target -----------------------------------------------------------------
dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Order status history, SCD Type 2, built from the file-based CDC feed.",
    cluster_by=["_insert_update_ts", "order_id"],
    expect_all_or_drop={"valid_order_id": "order_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target=TARGET_TABLE,
    source=SOURCE_VIEW,
    keys=["order_id"],
    sequence_by=F.col("_change_ts"),
    stored_as_scd_type=2,
)


# Paradigm note
#   Two source systems, two change mechanisms, one flow shape: the only thing
#   that changed from dp_scd2_customers is the sequence column and the absence of
#   a delete predicate. The classic track had to write a whole second
#   foreachBatch function to say the same thing.
#   The missing total_amount stays in place on both tracks, but only this one can
#   turn it into a published warn expectation later without touching the data.

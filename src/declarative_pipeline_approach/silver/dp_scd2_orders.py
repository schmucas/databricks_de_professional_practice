from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, TimestampType

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_TABLE = f"{CATALOG}.bronze.orders_raw"
TARGET_TABLE = "dp_scd2_orders"

SOURCE_VIEW = "dp_orders_changes"


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

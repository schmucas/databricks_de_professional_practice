from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, TimestampType

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_TABLE = f"{CATALOG}.bronze.customers_raw"
TARGET_TABLE = "dp_scd2_customers"

SOURCE_VIEW = "dp_customers_changes"


@dp.temporary_view(name=SOURCE_VIEW)
def dp_customers_changes():
    """Cast the bronze customer change feed to the classic silver column shape.

    Drops CDF pre-images, exactly as the classic foreachBatch does before it
    builds its own ordering window.

    Returns:
        Streaming DataFrame of customer change events carrying the business
        columns, the `_change_type` / `_commit_version` CDF metadata the flow
        sequences and deletes on, and `_insert_update_ts`.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        .filter(F.col("_change_type") != "update_preimage")
        .withColumn("postal_code", F.col("postal_code").cast(IntegerType()))
        .withColumn("signup_date", F.col("signup_date").cast(DateType()))
        .withColumn("last_updated", F.col("last_updated").cast(TimestampType()))
        .withColumn("_insert_update_ts", F.current_timestamp())
        .select(
            "customer_id",
            "company_name",
            "contact_email",
            "address",
            "city",
            "postal_code",
            "tier",
            "industry",
            "signup_date",
            "_insert_update_ts",
            "_commit_version",
            "last_updated",
            "_change_type",
        )
    )


dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Customer dimension history, SCD Type 2, built from the bronze CDF feed.",
    cluster_by=["_insert_update_ts", "customer_id"],
    expect_all_or_drop={"valid_customer_id": "customer_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target=TARGET_TABLE,
    source=SOURCE_VIEW,
    keys=["customer_id"],
    sequence_by=F.struct("last_updated", "_commit_version"),
    stored_as_scd_type=2,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
)

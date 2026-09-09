from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_TABLE = f"{CATALOG}.bronze.shipment_events_raw"
TARGET_TABLE = "dp_shipment_events"

SOURCE_VIEW = "dp_shipment_events_changes"
DEDUPE_SEQ = "_dedupe_seq"


@dp.temporary_view(name=SOURCE_VIEW)
def dp_shipment_events_changes():
    """Cast the bronze tracking-event feed to the classic silver column shape.

    Adds `_dedupe_seq`, the negated event time in microseconds, so that the auto
    CDC flow's last-writer-wins resolves to the earliest event per event_id,
    matching the classic ascending dedupe.

    Returns:
        Streaming DataFrame with the eleven classic columns plus
        `_insert_update_ts` and the `_dedupe_seq` helper.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("temperature_celsius", F.col("temperature_celsius").cast(DoubleType()))
        .withColumn("_insert_update_ts", F.current_timestamp())
        .withColumn(DEDUPE_SEQ, -F.unix_micros(F.col("event_timestamp")))
        .select(
            "event_id",
            "event_timestamp",
            "event_type",
            "latitude",
            "longitude",
            "order_id",
            "shipment_status",
            "source_system",
            "temperature_celsius",
            "vehicle_id",
            "event_date",
            "_insert_update_ts",
            DEDUPE_SEQ,
        )
    )


dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Shipment tracking events, deduplicated on event_id (earliest event_timestamp wins).",
    cluster_by=["_insert_update_ts", "vehicle_id", "shipment_status"],
    expect_all_or_drop={"valid_event_id": "event_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target=TARGET_TABLE,
    source=SOURCE_VIEW,
    keys=["event_id"],
    sequence_by=F.col(DEDUPE_SEQ),
    stored_as_scd_type=1,
    except_column_list=[DEDUPE_SEQ],
)

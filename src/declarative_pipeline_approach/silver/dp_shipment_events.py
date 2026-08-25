"""
Silver | dp_shipment_events | declarative track

Mirrors : src/classic_approach/silver/batch/silver_shipment_events.ipynb
Source  : bronze.shipment_events_raw  (high-volume event stream, Auto Loader)
Target  : dp_shipment_events  (append-only, deduplicated)

Parity notes
  - filters event_id IS NULL
  - roughly 3 percent duplicate event_ids, keep the earliest by event_timestamp
    (the classic notebook dedupes ascending on event_id / event_timestamp)
  - casts: event_timestamp TIMESTAMP, latitude DOUBLE, longitude DOUBLE,
           temperature_celsius DOUBLE
  - derived: event_date = to_date(event_timestamp)
  - column list: event_id, event_timestamp, event_type, latitude, longitude,
                 order_id, shipment_status, source_system, temperature_celsius,
                 vehicle_id, event_date

Dedupe on a streaming table is the interesting bit: an auto CDC flow with
stored_as_scd_type=1 gives idempotent last-writer-wins on event_id without
a stateful dropDuplicates.

Direction matters here. Auto CDC always keeps the highest sequence_by value,
but the classic notebook keeps the EARLIEST event_timestamp (dedupe_asc). So the
flow sequences by a negated event time (`_dedupe_seq`), which turns
"highest sequence wins" into "earliest event wins", and that helper column is
dropped from the target via except_column_list.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

# bronze is not the pipeline default schema, so it is qualified; silver is, so
# the target is named bare.
SOURCE_TABLE = f"{CATALOG}.bronze.shipment_events_raw"
TARGET_TABLE = "dp_shipment_events"

SOURCE_VIEW = "dp_shipment_events_changes"
DEDUPE_SEQ = "_dedupe_seq"


# --- source view ------------------------------------------------------------
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


# --- target -----------------------------------------------------------------
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


# Paradigm note
#   The classic dedupe only sees one batch: a duplicate that lands in a later run
#   is invisible to the window and gets merged in on top. Auto CDC keys on
#   event_id for the life of the table, so the dedupe holds across batches too.
#   What declarative gives up is the direction: "keep the latest" is native,
#   "keep the earliest" needs the negated sequence column above.

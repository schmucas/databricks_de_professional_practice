"""
Gold | dp_fact_shipment_event | declarative track

Mirrors : src/classic_approach/gold/gold_fact_shipment_event.ipynb
Sources : dp_shipment_events (silver), dp_dim_vehicle, dp_dim_date
Target  : dp_fact_shipment_event
Grain   : one row per tracking event  (PK event_id)

Parity notes
  - joins dim_vehicle on vehicle_id and dim_date on event_date, both LEFT
  - order_id rides along as a degenerate dimension, there is no order FK here
  - every other silver column passes through unchanged
  - drops the silver bookkeeping timestamp column

The append-only fact: silver is already deduplicated and never updates,
so this can be a streaming table rather than a materialized view. Worth
being deliberate about that choice, an interviewer will ask why.

Being deliberate about it lands on materialized view, not streaming table, and
the docstring premise is the thing to challenge. dp_shipment_events is an auto
CDC target, so it is NOT append-only: a duplicate event_id arriving in a later
batch rewrites the row that already won. A streaming read of it therefore fails
outright unless it carries skipChangeCommits=true, and with that option set the
fact would keep the superseded row forever. The dimensions are recomputed
materialized views too, so a streaming fact would also pin stale surrogate keys.

Known parity gap, agreed deliberately: the classic notebook aliases its
pass-through columns to "main.<name>", so the classic table's columns are
literally named `main.event_timestamp`, `main.latitude` and so on. That is a
typo for F.col(f"main.{c}"), which is what the telemetry fact does correctly.
This track uses the clean names. Until the classic notebook is patched, the two
fact tables differ in column NAMING (not in values, types, or order).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_EVENTS = "dp_shipment_events"
DIM_VEHICLE = "dp_dim_vehicle"
DIM_VEHICLE_FQN = f"{CATALOG}.gold.{DIM_VEHICLE}"
DIM_DATE = "dp_dim_date"
DIM_DATE_FQN = f"{CATALOG}.gold.{DIM_DATE}"
TARGET_TABLE = "dp_fact_shipment_event"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"


# --- target -----------------------------------------------------------------
@dp.materialized_view(
    name=TARGET_FQN,
    comment="Shipment tracking event fact, one row per event_id, conformed to dim_vehicle and dim_date.",
    cluster_by_auto=True,
    schema=f"""
        event_id STRING NOT NULL,
        vehicle_sk STRING,
        date_key INT,
        event_timestamp TIMESTAMP,
        event_type STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        order_id STRING,
        shipment_status STRING,
        source_system STRING,
        temperature_celsius DOUBLE,
        vehicle_id STRING,
        event_date DATE,
        CONSTRAINT dp_event_id_pk PRIMARY KEY (event_id),
        CONSTRAINT dp_fct_shipmt_vehicle_sk_fk FOREIGN KEY (vehicle_sk)
            REFERENCES {DIM_VEHICLE_FQN}(vehicle_sk),
        CONSTRAINT dp_fct_shipmt_date_key_fk FOREIGN KEY (date_key)
            REFERENCES {DIM_DATE_FQN}(date_key)
    """,
)
def dp_fact_shipment_event():
    """Join silver tracking events to the vehicle and date dimensions.

    Returns:
        Batch DataFrame with event_id, vehicle_sk, date_key and every remaining
        silver column, one row per tracking event.
    """
    silver_shipment_df = spark.read.table(SOURCE_EVENTS).drop("_insert_update_ts")
    dim_vehicle_df = spark.read.table(DIM_VEHICLE_FQN)
    dim_date_df = spark.read.table(DIM_DATE_FQN)

    return (
        silver_shipment_df.alias("main")
        .join(dim_vehicle_df.alias("v"), on="vehicle_id", how="left")
        .join(dim_date_df.alias("d"), on=(F.col("main.event_date") == F.col("d.full_date")), how="left")
        .select(
            "main.event_id",
            "v.vehicle_sk",
            "d.date_key",
            *[F.col(f"main.{c}") for c in silver_shipment_df.columns if c != "event_id"],
        )
    )


# Paradigm note
#   The interesting answer here is that "append-only fact, so make it a streaming
#   table" is the wrong instinct once silver is an auto CDC target: auto CDC
#   rewrites rows, and a streaming reader either errors or has to be told to
#   ignore exactly the corrections that make silver worth reading.
#   The cost of the materialized view is honest and real: classic MERGEs only the
#   events in its window, this recomputes the whole fact from silver every run.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_EVENTS = "dp_shipment_events"
DIM_VEHICLE = "dp_dim_vehicle"
DIM_VEHICLE_FQN = f"{CATALOG}.gold.{DIM_VEHICLE}"
DIM_DATE = "dp_dim_date"
DIM_DATE_FQN = f"{CATALOG}.gold.{DIM_DATE}"
TARGET_TABLE = "dp_fact_shipment_event"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"


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

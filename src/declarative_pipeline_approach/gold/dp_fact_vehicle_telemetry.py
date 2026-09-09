from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_TELEMETRY = "dp_vehicle_telemetry"
DIM_VEHICLE = "dp_dim_vehicle"
DIM_VEHICLE_FQN = f"{CATALOG}.gold.{DIM_VEHICLE}"
DIM_DATE = "dp_dim_date"
DIM_DATE_FQN = f"{CATALOG}.gold.{DIM_DATE}"
TARGET_TABLE = "dp_fact_vehicle_telemetry"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"

HOURLY_VIEW = "dp_telemetry_hourly"


@dp.temporary_view(name=HOURLY_VIEW)
def dp_telemetry_hourly():
    """Aggregate silver telemetry readings to one row per vehicle per hour.

    Returns:
        Batch DataFrame keyed by vehicle_id and period_start_timestamp with the
        classic measure set, km_driven, utilization_pct and period_date.
    """
    silver_telemetry_df = spark.read.table(SOURCE_TELEMETRY).drop("_insert_update_ts")

    return (
        silver_telemetry_df.withColumn("period_start_timestamp", F.date_trunc("hour", F.col("reading_timestamp")))
        .groupBy("vehicle_id", F.col("period_start_timestamp"))
        .agg(
            F.count(F.col("reading_id")).alias("readings_count"),
            F.round(F.avg(F.col("speed_kmh"))).alias("avg_speed_kmh"),
            F.max(F.col("speed_kmh")).alias("max_speed_kmh"),
            F.min(F.col("odometer_km")).alias("min_odometer_km"),
            F.max(F.col("odometer_km")).alias("max_odometer_km"),
            F.round(F.avg(F.col("fuel_pct")), 2).alias("avg_fuel_pct"),
            F.round(F.avg(F.col("engine_temp_c"))).alias("avg_engine_temp_c"),
            F.min(F.col("cargo_temp_c")).alias("min_cargo_temp_c"),
            F.max(F.col("cargo_temp_c")).alias("max_cargo_temp_c"),
            F.sum(F.when(F.col("vehicle_status").isin("idle", "maintenance"), F.lit(0)).otherwise(F.lit(1))).alias(
                "vehicle_idle"
            ),
        )
        .withColumn("km_driven", F.col("max_odometer_km") - F.col("min_odometer_km"))
        .withColumn("utilization_pct", F.round(F.col("vehicle_idle") / F.col("readings_count"), 2))
        .withColumn("period_date", F.col("period_start_timestamp").cast(DateType()))
        .drop("min_odometer_km", "max_odometer_km", "vehicle_idle")
    )


@dp.materialized_view(
    name=TARGET_FQN,
    comment="Hourly vehicle telemetry fact, conformed to dim_vehicle and dim_date.",
    cluster_by_auto=True,
    schema=f"""
        vehicle_id STRING NOT NULL,
        period_start_timestamp TIMESTAMP NOT NULL,
        vehicle_sk STRING,
        date_key INT,
        readings_count BIGINT,
        avg_speed_kmh DOUBLE,
        max_speed_kmh DOUBLE,
        avg_fuel_pct DOUBLE,
        avg_engine_temp_c DOUBLE,
        min_cargo_temp_c DOUBLE,
        max_cargo_temp_c DOUBLE,
        km_driven INT,
        utilization_pct DOUBLE,
        period_date DATE,
        CONSTRAINT dp_vehicle_telemetry_pk PRIMARY KEY (vehicle_id, period_start_timestamp),
        CONSTRAINT dp_fct_vhcl_vehicle_sk_fk FOREIGN KEY (vehicle_sk)
            REFERENCES {DIM_VEHICLE_FQN}(vehicle_sk),
        CONSTRAINT dp_fct_vhcl_date_key_fk FOREIGN KEY (date_key)
            REFERENCES {DIM_DATE_FQN}(date_key)
    """,
)
def dp_fact_vehicle_telemetry():
    """Join the hourly telemetry aggregate to the vehicle and date dimensions.

    Returns:
        Batch DataFrame with vehicle_id, period_start_timestamp, vehicle_sk,
        date_key and every remaining aggregate column.
    """
    telemetry_periodic = spark.read.table(HOURLY_VIEW)
    dim_vehicle_df = spark.read.table(DIM_VEHICLE_FQN)
    dim_date_df = spark.read.table(DIM_DATE_FQN)

    return (
        telemetry_periodic.alias("main")
        .join(dim_vehicle_df.alias("v"), "vehicle_id", "left")
        .join(dim_date_df.alias("d"), F.col("main.period_date") == F.col("d.full_date"), "left")
        .select(
            "main.vehicle_id",
            "main.period_start_timestamp",
            "v.vehicle_sk",
            "d.date_key",
            *[
                F.col(f"main.{c}")
                for c in telemetry_periodic.columns
                if c not in ["vehicle_id", "period_start_timestamp"]
            ],
        )
    )

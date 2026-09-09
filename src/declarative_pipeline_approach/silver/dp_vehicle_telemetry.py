from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_TABLE = f"{CATALOG}.bronze.vehicle_telemetry_raw"
TARGET_TABLE = "dp_vehicle_telemetry"

CAST_VIEW = "dp_vehicle_telemetry_cast"
SOURCE_VIEW = "dp_vehicle_telemetry_changes"
DEDUPE_SEQ = "_dedupe_seq"

SENSOR_RANGE_EXPECTATIONS = {
    "cargo_temp_c_in_range": "cargo_temp_c IS NULL OR cargo_temp_c BETWEEN -273 AND 100",
    "engine_temp_c_in_range": "engine_temp_c IS NULL OR engine_temp_c BETWEEN 0 AND 200",
    "speed_kmh_in_range": "speed_kmh IS NULL OR speed_kmh BETWEEN 0 AND 180",
}


@dp.temporary_view(name=CAST_VIEW)
@dp.expect_all(SENSOR_RANGE_EXPECTATIONS)
def dp_vehicle_telemetry_cast():
    """Cast the bronze telemetry feed, keeping out-of-range sensor values intact.

    The warn-level expectations above are evaluated here, before the values are
    nulled out, so the pipeline reports how many readings each sensor rule caught.

    Returns:
        Streaming DataFrame with the nine classic normalize columns, cast but
        not yet range-corrected.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("reading_timestamp", F.to_timestamp(F.col("reading_timestamp")))
        .withColumn("cargo_temp_c", F.col("cargo_temp_c").cast(DoubleType()))
        .withColumn("engine_temp_c", F.col("engine_temp_c").cast(DoubleType()))
        .withColumn("fuel_pct", F.col("fuel_pct").cast(DoubleType()))
        .withColumn("odometer_km", F.col("odometer_km").cast(IntegerType()))
        .withColumn("speed_kmh", F.col("speed_kmh").cast(DoubleType()))
        .select(
            "cargo_temp_c",
            "engine_temp_c",
            "fuel_pct",
            "odometer_km",
            "reading_id",
            "reading_timestamp",
            "speed_kmh",
            "vehicle_id",
            "vehicle_status",
        )
    )


@dp.temporary_view(name=SOURCE_VIEW)
def dp_vehicle_telemetry_changes():
    """Null out-of-range sensor values, then enrich, mirroring the classic transforms.

    Adds `_dedupe_seq`, the negated reading time in microseconds, so the auto CDC
    flow's last-writer-wins resolves to the earliest reading per reading_id.

    Returns:
        Streaming DataFrame with the thirteen classic columns plus
        `_insert_update_ts` and the `_dedupe_seq` helper.
    """
    return (
        spark.readStream.table(CAST_VIEW)
        .withColumn(
            "cargo_temp_c",
            F.when((F.col("cargo_temp_c") < -273) | (F.col("cargo_temp_c") > 100), F.lit(None)).otherwise(
                F.col("cargo_temp_c")
            ),
        )
        .withColumn(
            "engine_temp_c",
            F.when((F.col("engine_temp_c") < 0) | (F.col("engine_temp_c") > 200), F.lit(None)).otherwise(
                F.col("engine_temp_c")
            ),
        )
        .withColumn(
            "speed_kmh",
            F.when((F.col("speed_kmh") < 0) | (F.col("speed_kmh") > 180), F.lit(None)).otherwise(F.col("speed_kmh")),
        )
        .withColumn(
            "is_cold_chain_cargo",
            F.when(F.col("cargo_temp_c").isNotNull(), True).otherwise(F.lit(False)),
        )
        .withColumn("speed_limit_exceed", F.when(F.col("speed_kmh") > 120, True).otherwise(False))
        .withColumn("engine_overheat", F.when(F.col("engine_temp_c") > 110, True).otherwise(False))
        .withColumn("reading_date", F.to_date(F.col("reading_timestamp")))
        .withColumn("_insert_update_ts", F.current_timestamp())
        .withColumn(DEDUPE_SEQ, -F.unix_micros(F.col("reading_timestamp")))
    )


dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Vehicle telemetry readings, range-corrected and deduplicated on reading_id.",
    cluster_by=["_insert_update_ts", "vehicle_id", "vehicle_status"],
    expect_all_or_drop={"valid_reading_id": "reading_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target=TARGET_TABLE,
    source=SOURCE_VIEW,
    keys=["reading_id"],
    sequence_by=F.col(DEDUPE_SEQ),
    stored_as_scd_type=1,
    except_column_list=[DEDUPE_SEQ],
)

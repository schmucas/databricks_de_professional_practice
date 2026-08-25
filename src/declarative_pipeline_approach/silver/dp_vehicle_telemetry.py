"""
Silver | dp_vehicle_telemetry | declarative track

Mirrors : src/classic_approach/silver/batch/silver_vehicle_telemetry.ipynb
          (plus silver_vehicle_telemetry_transforms.py)
Source  : bronze.vehicle_telemetry_raw  (IoT, at-least-once delivery, Auto Loader)
Target  : dp_vehicle_telemetry  (append-only, deduplicated, enriched)

Parity notes, normalize step
  - filters reading_id IS NULL, dedupe key is reading_id (timestamps are NOT unique)
  - casts: reading_timestamp TIMESTAMP, cargo_temp_c DOUBLE, engine_temp_c DOUBLE,
           fuel_pct DOUBLE, odometer_km INT, speed_kmh DOUBLE
  - out-of-range sensor values become NULL:
        cargo_temp_c  outside [-273, 100]
        engine_temp_c outside [0, 200]
        speed_kmh     outside [0, 180]

Parity notes, enrich step
  - is_cold_chain_cargo = cargo_temp_c IS NOT NULL
  - speed_limit_exceed  = speed_kmh > 120
  - engine_overheat     = engine_temp_c > 110
  - reading_date        = to_date(reading_timestamp)

The classic track puts the range checks in Python and keeps the row.
The declarative version should express them as expectations, so the same
rules also show up as pipeline data-quality metrics.

That needs two views, not one. The expectations sit on the cast-only view, where
the out-of-range values still exist and can be counted; the second view then
nulls them out exactly as the classic transform does. Nulling first would make
the expectations unfalsifiable, and every range check would report 100 percent
pass on data the classic track knows is bad.

Like dp_shipment_events, the classic dedupe is ascending (earliest
reading_timestamp per reading_id wins), so the flow sequences by a negated
event time.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

# bronze is not the pipeline default schema, so it is qualified; silver is, so
# the target is named bare.
SOURCE_TABLE = f"{CATALOG}.bronze.vehicle_telemetry_raw"
TARGET_TABLE = "dp_vehicle_telemetry"

CAST_VIEW = "dp_vehicle_telemetry_cast"
SOURCE_VIEW = "dp_vehicle_telemetry_changes"
DEDUPE_SEQ = "_dedupe_seq"

# Sensor plausibility ranges, identical to the classic normalize step. NULL passes:
# roughly 70 percent of readings carry no cargo_temp_c at all and that is expected.
SENSOR_RANGE_EXPECTATIONS = {
    "cargo_temp_c_in_range": "cargo_temp_c IS NULL OR cargo_temp_c BETWEEN -273 AND 100",
    "engine_temp_c_in_range": "engine_temp_c IS NULL OR engine_temp_c BETWEEN 0 AND 200",
    "speed_kmh_in_range": "speed_kmh IS NULL OR speed_kmh BETWEEN 0 AND 180",
}


# --- source views -----------------------------------------------------------
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
        # normalize: implausible sensor values become NULL, the row is kept
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
        # enrich
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


# --- target -----------------------------------------------------------------
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


# Paradigm note
#   Classic nulls a bad sensor value and moves on; nobody downstream can tell a
#   sensor fault from a reading that was legitimately absent. Expectations keep
#   the same output but publish the counts, so "engine_temp_c went out of range
#   4,000 times today" becomes visible without writing a single audit query.
#   What declarative will not give back is the skew hint the gold aggregate needs
#   (see dp_fact_vehicle_telemetry): the planner is not ours to steer here.

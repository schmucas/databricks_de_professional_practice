from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.testing import assertDataFrameEqual
from src.classic_approach.silver.batch.silver_vehicle_telemetry_transforms import normalize_vehicle_telemetry, enrich_vehicle_telemetry

def test_normalize_vehicle_telemetry(spark):
    data = [
        # Valid row
        (1, "2026-06-24 10:00:00", 5.0, 90.0, 50.0, 10000, 80.0, "V001", "active"),
        # cargo_temp_c < -273 (invalid)
        (2, "2026-06-24 11:00:00", -300.0, 100.0, 60.0, 20000, 100.0, "V002", "inactive"),
        # cargo_temp_c > 100 (invalid)
        (3, "2026-06-24 12:00:00", 120.0, 100.0, 70.0, 30000, 100.0, "V003", "active"),
        # engine_temp_c < 0 (invalid)
        (4, "2026-06-23 12:00:00", 10.0, -10.6, 70.0, 30000, 100.0, "V004", "active"),
        # engine_temp_c > 200 (invalid)
        (5, "2026-06-23 13:00:00", 10.0, 250.0, 70.0, 30000, 100.0, "V005", "active"),
        # speed_kmh < 0 (invalid)
        (6, "2026-06-23 14:00:00", 10.0, 100.0, 70.0, 30000, -5.0, "V006", "active"),
        # speed_kmh > 180 (invalid)
        (7, "2026-06-23 15:00:00", 10.0, 100.0, 70.0, 30000, 200.0, "V007", "active"),
        # reading_id is None (should be filtered out)
        (None, "2026-06-23 16:00:00", 10.0, 100.0, 70.0, 30000, 100.0, "V008", "active"),
    ]
    columns = [
        "reading_id", "reading_timestamp", "cargo_temp_c", "engine_temp_c",
        "fuel_pct", "odometer_km", "speed_kmh", "vehicle_id", "vehicle_status"
    ]
    df = spark.createDataFrame(data, columns)
    result = normalize_vehicle_telemetry(df)
    
    expected_data = [
        (5.0, 90.0, 50.0, 10000, 1, "2026-06-24 10:00:00", 80.0, "V001", "active"),
        (None, 100.0, 60.0, 20000, 2, "2026-06-24 11:00:00", 100.0, "V002", "inactive"),
        (None, 100.0, 70.0, 30000, 3, "2026-06-24 12:00:00", 100.0, "V003", "active"),
        (10.0, None, 70.0, 30000, 4, "2026-06-23 12:00:00", 100.0, "V004", "active"),
        (10.0, None, 70.0, 30000, 5, "2026-06-23 13:00:00", 100.0, "V005", "active"),
        (10.0, 100.0, 70.0, 30000, 6, "2026-06-23 14:00:00", None, "V006", "active"),
        (10.0, 100.0, 70.0, 30000, 7, "2026-06-23 15:00:00", None, "V007", "active"),
    ]
    expected_columns = [
        "cargo_temp_c", "engine_temp_c", "fuel_pct", "odometer_km",
        "reading_id", "reading_timestamp", "speed_kmh", "vehicle_id", "vehicle_status"
    ]
    expected = spark.createDataFrame(expected_data, expected_columns)
    expected = expected.withColumn('reading_timestamp', F.to_timestamp(F.col('reading_timestamp')))
    expected = expected.withColumn('odometer_km', F.col('odometer_km').cast(IntegerType()))
    
    assertDataFrameEqual(result, expected)


def test_enrich_vehicle_telemetry(spark):
    data = [
        # cold chain cargo, speed under limit, engine normal
        (5.0, 90.0, 50.0, 10000, 1, "2026-06-24 10:00:00", 80.0, "V001", "active"),
        # not cold chain, speed over limit, engine normal
        (None, 120.0, 60.0, 20000, 2, "2026-06-24 11:00:00", 130.0, "V002", "inactive"),
        # cold chain, speed null, engine null
        (10.0, None, 70.0, 30000, 3, "2026-06-24 12:00:00", None, "V003", "active"),
        # cold chain, speed at limit, engine overheat
        (8.0, 150.0, 80.0, 40000, 4, "2026-06-24 13:00:00", 120.0, "V004", "active"),
        # not cold chain, speed null, engine overheat
        (None, 180.0, 90.0, 50000, 5, "2026-06-24 14:00:00", None, "V005", "inactive"),
    ]
    columns = [
        "cargo_temp_c", "engine_temp_c", "fuel_pct", "odometer_km",
        "reading_id", "reading_timestamp", "speed_kmh", "vehicle_id", "vehicle_status"
    ]
    df = spark.createDataFrame(data, columns)
    df = df.withColumn('reading_timestamp', F.to_timestamp(F.col('reading_timestamp')))
    result = enrich_vehicle_telemetry(df)

    expected_data = [
        # is_cold_chain_cargo, speed_limit_exceed, engine_overheat, reading_date
        (5.0, 90.0, 50.0, 10000, 1, "2026-06-24 10:00:00", 80.0, "V001", "active", True, False, False, "2026-06-24"),
        (None, 120.0, 60.0, 20000, 2, "2026-06-24 11:00:00", 130.0, "V002", "inactive", False, True, True, "2026-06-24"),
        (10.0, None, 70.0, 30000, 3, "2026-06-24 12:00:00", None, "V003", "active", True, False, False, "2026-06-24"),
        (8.0, 150.0, 80.0, 40000, 4, "2026-06-24 13:00:00", 120.0, "V004", "active", True, False, True, "2026-06-24"),
        (None, 180.0, 90.0, 50000, 5, "2026-06-24 14:00:00", None, "V005", "inactive", False, False, True, "2026-06-24"),
    ]
    expected_columns = [
        "cargo_temp_c", "engine_temp_c", "fuel_pct", "odometer_km",
        "reading_id", "reading_timestamp", "speed_kmh", "vehicle_id", "vehicle_status",
        "is_cold_chain_cargo", "speed_limit_exceed", "engine_overheat", "reading_date"
    ]
    expected = spark.createDataFrame(expected_data, expected_columns)
    expected = expected.withColumn('reading_timestamp', F.to_timestamp(F.col('reading_timestamp')))
    expected = expected.withColumn('reading_date', F.to_date(F.col('reading_date')))

    assertDataFrameEqual(result, expected)
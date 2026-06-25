# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# COMMAND ----------
def normalize_vehicle_telemetry(df: DataFrame) -> DataFrame:
    return (df.filter(F.col("reading_id").isNotNull())
        .withColumn('reading_timestamp', F.to_timestamp(F.col("reading_timestamp")))
            .withColumn('cargo_temp_c', F.col("cargo_temp_c").cast(DoubleType()))
            .withColumn('cargo_temp_c', 
                        F.when((F.col("cargo_temp_c") < -273) | (F.col("cargo_temp_c") > 100), F.lit(None))
                        .otherwise(F.col("cargo_temp_c")))
            .withColumn('engine_temp_c', F.col("engine_temp_c").cast(DoubleType()))
            .withColumn('engine_temp_c', 
                        F.when((F.col("engine_temp_c") < 0) | (F.col("engine_temp_c") > 200), F.lit(None))
                        .otherwise(F.col("engine_temp_c")))
            .withColumn('fuel_pct', F.col("fuel_pct").cast(DoubleType()))
            .withColumn('odometer_km', F.col("odometer_km").cast(IntegerType()))
            .withColumn('speed_kmh', F.col("speed_kmh").cast(DoubleType()))
            .withColumn('speed_kmh', 
                        F.when((F.col("speed_kmh") < 0) | (F.col("speed_kmh") > 180), F.lit(None))
                        .otherwise(F.col("speed_kmh")))
            .select("cargo_temp_c",
                        "engine_temp_c",
                        "fuel_pct",
                        "odometer_km",
                        "reading_id",
                        "reading_timestamp",
                        "speed_kmh",
                        "vehicle_id",
                        "vehicle_status"))

# COMMAND ----------
def enrich_vehicle_telemetry(df: DataFrame) -> DataFrame:
    return (df
            .withColumn('is_cold_chain_cargo', F.when(F.col("cargo_temp_c").isNotNull(), True).otherwise(F.lit(False)))
            .withColumn('speed_limit_exceed', F.when(F.col("speed_kmh") > 120, True).otherwise(False))
            .withColumn('engine_overheat', F.when(F.col("engine_temp_c") > 110, True).otherwise(False))
            .withColumn('reading_date', F.to_date(F.col("reading_timestamp")))
            )

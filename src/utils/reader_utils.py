# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

# COMMAND ----------
def dedupe(df: DataFrame, partition_cols: list, order_col: str, desc=True) -> DataFrame:
    window_spec = (Window
                   .partitionBy(*partition_cols)
                   .orderBy(F.col(order_col).desc() if desc else F.col(order_col).asc()))
    
    return (df.withColumn("row_order", F.row_number().over(window_spec))
                .filter(F.col("row_order") == 1)
                .drop("row_order"))



# COMMAND ----------
def read_delta_table(table_conf: dict, start_date: str, end_date: str) -> DataFrame:
    table = table_conf.get("table")
    schema = table_conf.get("schema")
    ts_col = table_conf.get("timestamp_col")
    order_col = table_conf.get("order_col")

    df = spark.read.table(f"{schema}.{table}").filter(F.col(ts_col).between(start_date, end_date))  # type: ignore[name-defined]
    
    if "dedupe_desc" in table_conf:
        return df.transform(dedupe, table_conf.get("dedupe_desc"), order_col)
    
    if "dedupe_asc" in table_conf:
        return df.transform(dedupe, table_conf.get("dedupe_asc"), order_col, False)
    
    return df
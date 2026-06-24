# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

# COMMAND ----------
def dedupe(df: DataFrame, partition_cols: list, order_col: str, desc=True) -> DataFrame:
    """Deduplicates a DataFrame by keeping the first row per partition based on a sort order.

    Args:
        df: Input DataFrame to deduplicate.
        partition_cols: Columns to partition by when assigning row numbers.
        order_col: Column used to order rows within each partition.
        desc: If True, rows are ordered descending (latest first). Defaults to True.

    Returns:
        DataFrame with one row per unique combination of partition_cols,
        retaining the row ranked first by order_col.
    """
    window_spec = (Window
                   .partitionBy(*partition_cols)
                   .orderBy(F.col(order_col).desc() if desc else F.col(order_col).asc()))
    
    return (df.withColumn("row_order", F.row_number().over(window_spec))
                .filter(F.col("row_order") == 1)
                .drop("row_order"))



# COMMAND ----------
def read_delta_table(table_conf: dict, start_date: str, end_date: str) -> DataFrame:
    """Reads a Delta table from Unity Catalog and applies a timestamp filter and optional deduplication.

    The table config dict controls which table to read and how to deduplicate.
    If neither ``dedupe_desc`` nor ``dedupe_asc`` is present, the filtered
    DataFrame is returned as-is.

    Args:
        table_conf: Configuration dict with the following keys:
            - ``table`` (str): Table name.
            - ``schema`` (str): Schema (database) name.
            - ``timestamp_col`` (str): Column used for the date range filter.
            - ``order_col`` (str): Column used to order rows during deduplication.
            - ``dedupe_desc`` (list, optional): Partition columns for descending dedup.
            - ``dedupe_asc`` (list, optional): Partition columns for ascending dedup.
        start_date: Inclusive lower bound for the timestamp filter (ISO date string).
        end_date: Inclusive upper bound for the timestamp filter (ISO date string).

    Returns:
        Filtered (and optionally deduplicated) DataFrame.
    """
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
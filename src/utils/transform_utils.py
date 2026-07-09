# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# COMMAND ----------
def generate_sk(df: DataFrame, sk_name: str, column_list: list):
    """Add an MD5 surrogate key column computed from the given non null columns.

    Args:
        df: Source DataFrame.
        sk_name: Name of the surrogate key column to create.
        column_list: Columns whose values are concatenated (delimited by "|")
            and hashed to produce the surrogate key. Pass a stable column order;
            reordering changes the key.

    Note:
        Timestamp/date columns are rendered via CAST AS STRING in the Spark
        session time zone (Databricks default: UTC). Keep
        spark.sql.session.timeZone stable or keys are not reproducible.

    Returns:
        DataFrame with `sk_name` prepended to the original columns.
    """
    original_cols = df.columns
    delimiter = "|"

    # concat columns with delimiter in between each column and hash with md5 algo
    hashed_df = (df
                 .withColumn(sk_name, F.md5(
                     F.concat_ws(delimiter, *[F.col(c).cast(StringType()) for c in column_list])))
                 )

    return (hashed_df.select(
                sk_name, *original_cols
            ))

# COMMAND ----------
def update_insert_update_ts(df: DataFrame):
    return df.withColumn("_insert_update_ts", F.current_timestamp())

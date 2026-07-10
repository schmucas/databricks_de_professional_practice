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
def add_or_update_insert_update_ts(df: DataFrame):
    """Add or overwrite a `_insert_update_ts` column with the current timestamp.

    Args:
        df: Source DataFrame.

    Returns:
        DataFrame with `_insert_update_ts` set to the timestamp at which this
        transformation runs.
    """
    return df.withColumn("_insert_update_ts", F.current_timestamp())

# COMMAND ----------
def add_normalized_str_col(df: DataFrame, col_to_normalize: str, col_name: str= "nrm_col")-> DataFrame:
    """Add a lowercased, accent-stripped, alphanumeric-only version of a column.

    Args:
        df: Source DataFrame.
        col_to_normalize: Name of the column to normalize.
        col_name: Name of the new normalized column. Defaults to "nrm_col".

    Note:
        Only the accented Latin characters listed in the translate map are
        folded to their unaccented equivalent; any other character outside
        A-Z/a-z/0-9/space is dropped rather than transliterated.

    Returns:
        DataFrame with `col_name` added.
    """
    return df.withColumn(
            col_name,
            F.lower(
                F.regexp_replace(
                    F.translate(
                        F.col(col_to_normalize),
                        "àâäéèêëïîôöûüçÀÂÄÉÈÊËÏÎÔÖÛÜÇ",
                        "aaaeeeeiioouucAAAEEEEIIOOUUC",
                        ),
                        "[^A-Za-z0-9 ]", ""
                    )
                )
            )

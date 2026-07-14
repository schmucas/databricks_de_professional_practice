# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# COMMAND ----------
def get_merge_keys(keys: list) -> str:
    """Builds a SQL merge condition string from a list of join key column names.

    Args:
        keys: Column names to use as merge keys.

    Returns:
        A multi-line SQL condition string of the form
        ``source.<key> = target.<key>`` joined by ``\n and ``.
    """
    conditions = [f"source.{k} = target.{k}" for k in keys]
    return "\n and ".join(conditions)

# COMMAND ----------
def upsert_to_delta(df: DataFrame, table_conf: dict):
    """Merges a DataFrame into a Unity Catalog Delta table using a matched-update / not-matched-insert strategy.

    Args:
        df: Source DataFrame whose rows will be merged into the target table.
        table_conf: Configuration dict with the following keys:
            - ``table`` (str): Target table name.
            - ``schema`` (str): Target schema (database) name.
            - ``merge_keys`` (list[str]): Columns used to match source rows to target rows.
    """
    keys = get_merge_keys(table_conf.get('merge_keys'))
    target_table = DeltaTable.forName(spark, f"{table_conf.get('schema')}.{table_conf.get('table')}")

    (target_table.alias('target')
    .merge(
        df.alias('source'),
        keys
    ).whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# COMMAND ----------
def delete_in_delta_table(df: DataFrame, table_conf: dict):
    """Deletes rows from a Unity Catalog Delta table that match rows in a DataFrame.

    Args:
        df: Source DataFrame whose matching rows will be deleted from the target table.
        table_conf: Configuration dict with the following keys:
            - ``table`` (str): Target table name.
            - ``schema`` (str): Target schema (database) name.
            - ``merge_keys`` (list[str]): Columns used to match source rows to target rows.
    """
    keys = get_merge_keys(table_conf.get('merge_keys'))
    target_table = DeltaTable.forName(spark, f"{table_conf.get('schema')}.{table_conf.get('table')}")

    (target_table.alias('target')
    .merge(
        df.alias('source'),
        keys
    ).whenMatchedDelete()
    .execute())
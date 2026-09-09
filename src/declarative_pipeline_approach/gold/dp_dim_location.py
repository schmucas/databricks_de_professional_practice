from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_ORDERS = "dp_scd2_orders"
SOURCE_LOOKUP = "dp_static_location_lookup"
TARGET_TABLE = "dp_dim_location"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"


def _generate_sk(df: DataFrame, sk_name: str, column_list: list) -> DataFrame:
    """Add an MD5 surrogate key column computed from the given columns.

    Byte-for-byte equivalent to utils.transform_utils.generate_sk, which the
    classic notebooks pull in with %run. Notebook magic does not exist in a
    pipeline source file, so the recipe is restated here: values cast to string,
    joined by "|", hashed with md5.

    Args:
        df: Source DataFrame.
        sk_name: Name of the surrogate key column to create.
        column_list: Columns whose values are concatenated (delimited by "|")
            and hashed. Pass a stable column order; reordering changes the key.

    Returns:
        DataFrame with `sk_name` prepended to the original columns.
    """
    original_cols = df.columns
    delimiter = "|"

    hashed_df = df.withColumn(
        sk_name,
        F.md5(F.concat_ws(delimiter, *[F.col(c).cast(StringType()) for c in column_list])),
    )

    return hashed_df.select(sk_name, *original_cols)


def _add_normalized_str_col(df: DataFrame, col_to_normalize: str, col_name: str = "nrm_col") -> DataFrame:
    """Add a lowercased, accent-stripped, alphanumeric-only version of a column.

    Byte-for-byte equivalent to utils.transform_utils.add_normalized_str_col.
    Only the accented Latin characters in the translate map are folded; any other
    character outside A-Z/a-z/0-9/space is dropped rather than transliterated.

    Args:
        df: Source DataFrame.
        col_to_normalize: Name of the column to normalize.
        col_name: Name of the new normalized column. Defaults to "nrm_col".

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
                "[^A-Za-z0-9 ]",
                "",
            )
        ),
    )


@dp.materialized_view(
    name=TARGET_FQN,
    comment="Location dimension: distinct order cities enriched from the static Middle-earth lookup.",
    cluster_by_auto=True,
    schema="""
        location_sk STRING NOT NULL,
        city STRING,
        realm_name STRING,
        realm_code STRING,
        region STRING,
        language_region STRING,
        _insert_update_ts TIMESTAMP,
        CONSTRAINT dp_location_sk_pk PRIMARY KEY (location_sk)
    """,
)
def dp_dim_location():
    """Build the location dimension from the cities seen on orders.

    Returns:
        Batch DataFrame with location_sk, city, realm_name, realm_code, region,
        language_region and _insert_update_ts, one row per distinct city plus a
        single collapsed 'unknown' row.
    """
    cities_df = (
        spark.read.table(SOURCE_ORDERS)
        .select("origin_city", "destination_city")
        .filter(
            (F.col("origin_city").isNotNull())
            & (F.col("origin_city") != "")
            & (F.col("destination_city").isNotNull())
            & (F.col("destination_city") != "")
        )
    )

    cities_deduped_df = (
        cities_df.select("origin_city")
        .union(cities_df.select("destination_city"))
        .dropDuplicates()
        .transform(_add_normalized_str_col, "origin_city")
    )

    static_location_lookup_df = spark.read.table(SOURCE_LOOKUP).transform(_add_normalized_str_col, "city")

    location_df = (
        cities_deduped_df.alias("cty")
        .join(F.broadcast(static_location_lookup_df), "nrm_col", "left")
        .select(F.col("cty.origin_city"), *static_location_lookup_df.columns)
        .withColumn("missing_lookup", F.when(F.col("city").isNull(), F.lit("unknown")).otherwise(F.lit(None)))
        .withColumn("city", F.coalesce("city", "missing_lookup"))
        .drop("nrm_col", "origin_city", "missing_lookup")
        .dropDuplicates()
    )

    return location_df.transform(_generate_sk, "location_sk", ["city"]).withColumn(
        "_insert_update_ts", F.current_timestamp()
    )

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_TABLE = "dp_scd2_customers"
TARGET_TABLE = "dp_dim_customer"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"

START_AT = "__START_AT.last_updated"


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


@dp.materialized_view(
    name=TARGET_FQN,
    comment="Customer dimension, SCD Type 2, keyed by an md5 surrogate on (customer_id, start_at).",
    cluster_by_auto=True,
    schema="""
        customer_sk STRING NOT NULL,
        customer_id INT,
        company_name STRING,
        contact_email STRING,
        address STRING,
        city STRING,
        postal_code INT,
        tier STRING,
        industry STRING,
        signup_date DATE,
        __START_AT STRUCT<last_updated: TIMESTAMP, _commit_version: BIGINT>,
        __END_AT STRUCT<last_updated: TIMESTAMP, _commit_version: BIGINT>,
        _insert_update_ts TIMESTAMP,
        _commit_version BIGINT,
        CONSTRAINT dp_customer_sk_pk PRIMARY KEY (customer_sk)
    """,
)
def dp_dim_customer():
    """Project the silver customer history into the customer dimension.

    Returns:
        Batch DataFrame with customer_sk, the nine business columns, the native
        SCD2 validity columns and the bookkeeping columns, in the classic
        column order.
    """
    return (
        spark.read.table(SOURCE_TABLE)
        .transform(_generate_sk, "customer_sk", ["customer_id", START_AT])
        .withColumn("_insert_update_ts", F.current_timestamp())
        .select(
            "customer_sk",
            "customer_id",
            "company_name",
            "contact_email",
            "address",
            "city",
            "postal_code",
            "tier",
            "industry",
            "signup_date",
            "__START_AT",
            "__END_AT",
            "_insert_update_ts",
            "_commit_version",
        )
    )

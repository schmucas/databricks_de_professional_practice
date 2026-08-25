"""
Gold | dp_dim_customer | declarative track

Mirrors : src/classic_approach/gold/gold_dim_customer.ipynb
Source  : dp_scd2_customers  (silver)
Target  : dp_dim_customer  (SCD Type 2 dimension)

Parity notes
  - excludes rows flagged as deleted at silver, they must not sink into gold
  - customer_sk = surrogate key hashed from (customer_id, start_at),
    same recipe as the classic generate_sk helper
  - carries the SCD2 columns through: start_at, end_at, is_current
  - business columns: customer_id, company_name, contact_email, address, city,
                      postal_code, tier, industry, signup_date

Because silver already historized the entity, gold is a projection plus a
surrogate key, so a materialized view is enough here.

Two of those parity notes cannot be met, and both trace back to the same root:
the silver auto CDC flow owns the SCD2 columns, so this track has __START_AT /
__END_AT and no is_current, and no _is_delete flag either. See the paradigm
note at the bottom.

The surrogate key IS at parity. The classic hash input is start_at, and the
declarative equivalent of that exact timestamp is __START_AT.last_updated
(the first field of the sequencing struct), not the struct itself. Hashing the
struct would stringify it as "{2025-01-01 00:00:00, 12}" and every customer_sk
in the comparison would diverge.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_TABLE = "dp_scd2_customers"
TARGET_TABLE = "dp_dim_customer"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"

# The classic hash input, spelled the way this track stores it.
START_AT = "__START_AT.last_updated"


# --- surrogate key ----------------------------------------------------------
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


# --- target -----------------------------------------------------------------
@dp.materialized_view(
    name=TARGET_FQN,
    comment="Customer dimension, SCD Type 2, keyed by an md5 surrogate on (customer_id, start_at).",
    cluster_by_auto=True,
    # __START_AT / __END_AT take the type of the silver flow's sequence_by, which
    # is struct(last_updated, _commit_version). Change that sequence and this
    # schema string has to change with it.
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


# Paradigm note
#   The classic track carries a _is_delete flag from silver, filters it out here,
#   and then needs gold_delete_customer_downstream.ipynb to chase the already
#   written rows out of gold. None of that exists on this track: the auto CDC
#   flow applied the delete at silver, and this view is recomputed from silver
#   every refresh, so gold cannot hold a row silver no longer justifies.
#   Be precise about what "applied the delete" means under SCD Type 2 though. It
#   closes the churned customer's current row, it does not erase their history,
#   so their past rows still appear here with __END_AT set. That is right for a
#   Type 2 dimension and wrong for a right-to-erasure request, which would need
#   a Type 1 flow or a UC purge either way.

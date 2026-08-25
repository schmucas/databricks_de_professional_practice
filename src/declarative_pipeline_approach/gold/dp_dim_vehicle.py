"""
Gold | dp_dim_vehicle | declarative track

Mirrors : src/classic_approach/gold/gold_dim_vehicle.ipynb
Source  : dp_scd1_vehicles  (silver)
Target  : dp_dim_vehicle  (SCD Type 1 dimension)

Parity notes
  - vehicle_sk = surrogate key hashed from (vehicle_id)
  - current state only, no start_at / end_at / is_current
  - business columns: vehicle_id, plate_number, model, vehicle_type, capacity_kg,
                      cold_chain_capable, home_depot, commissioned_date

The thinnest table in the track: silver already holds current state, so gold
adds a surrogate key and nothing else.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_TABLE = "dp_scd1_vehicles"
TARGET_TABLE = "dp_dim_vehicle"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"


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
    comment="Vehicle dimension, SCD Type 1, keyed by an md5 surrogate on vehicle_id.",
    cluster_by_auto=True,
    schema="""
        vehicle_sk STRING NOT NULL,
        capacity_kg INT,
        cold_chain_capable BOOLEAN,
        commissioned_date DATE,
        home_depot STRING,
        last_updated TIMESTAMP,
        model STRING,
        plate_number STRING,
        vehicle_id STRING,
        vehicle_type STRING,
        _insert_update_ts TIMESTAMP,
        CONSTRAINT dp_vehicle_sk_pk PRIMARY KEY (vehicle_sk)
    """,
)
def dp_dim_vehicle():
    """Project the silver fleet master into the vehicle dimension.

    Returns:
        Batch DataFrame with vehicle_sk followed by every silver column, one
        row per vehicle.
    """
    return (
        spark.read.table(SOURCE_TABLE)
        .transform(_generate_sk, "vehicle_sk", ["vehicle_id"])
        .withColumn("_insert_update_ts", F.current_timestamp())
    )


# Paradigm note
#   Identical output, opposite mechanics: classic MERGEs the batch it happens to
#   have read into whatever gold already holds, so gold is only as correct as the
#   window the job was handed. The materialized view derives gold from all of
#   silver every time, which makes a wrong START_DATE unable to corrupt it.
#   The surrogate key is the one thing declarative does not simplify: the hash
#   had to be copied here because %run has no pipeline equivalent.

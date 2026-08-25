"""
Gold | dp_dim_date | declarative track

Mirrors : src/classic_approach/gold/gold_dim_date.ipynb
Source  : generated, no upstream table
Target  : dp_dim_date  (conformed date dimension, shared by all three facts)

Parity notes
  - range: 2020-01-01 through today, built with sequence() + explode
  - date_key = date_format(full_date, 'yyyyMMdd') cast to INT  (the PK)
  - columns: date_key, full_date, year, month, month_name, iso_week,
             day_of_week, is_weekend
  - the classic notebook also lists quarter / day_name / is_holiday in the
    README star schema but does not build them yet, keep that parity, do not
    silently add columns the classic track lacks

Generated tables are awkward in a declarative pipeline: the range has to be
recomputed each refresh rather than appended. A full-refresh materialized
view is the honest shape.

One deliberate difference from the classic notebook: it reads max(full_date)
off its own target and generates only the gap since the last run, which makes
the table its own upstream dependency. A materialized view cannot read itself,
so the range is always regenerated from EARLIEST_DATE. Same rows, no
self-reference.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

TARGET_TABLE = "dp_dim_date"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"
EARLIEST_DATE = "2020-01-01"


# --- target -----------------------------------------------------------------
@dp.materialized_view(
    name=TARGET_FQN,
    comment="Conformed date dimension from 2020-01-01 to the current date.",
    cluster_by_auto=True,
    schema="""
        date_key INT NOT NULL,
        full_date DATE,
        year INT,
        month INT,
        month_name STRING,
        iso_week INT,
        day_of_week INT,
        is_weekend BOOLEAN,
        CONSTRAINT dp_date_key_pk PRIMARY KEY (date_key)
    """,
)
def dp_dim_date():
    """Generate the conformed date dimension.

    Returns:
        Batch DataFrame with one row per calendar day, columns date_key,
        full_date, year, month, month_name, iso_week, day_of_week and
        is_weekend.
    """
    date_range_df = spark.sql(f"SELECT explode(sequence(DATE '{EARLIEST_DATE}', current_date())) AS full_date")

    return (
        date_range_df.withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()))
        .select(F.col("date_key"), F.col("full_date"))
        .withColumn("year", F.year(F.col("full_date")))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("iso_week", F.weekofyear("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
    )


# Paradigm note
#   The classic notebook needs an INITIAL_RUN branch to decide "seed the whole
#   range" versus "extend from max(full_date)"; the materialized view has one
#   code path because recompute is the only path. Cheaper to reason about,
#   more expensive to run: roughly 2,400 rows regenerated every refresh instead
#   of the one row a day the incremental version writes.

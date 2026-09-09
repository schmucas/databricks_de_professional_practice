from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

TARGET_TABLE = "dp_dim_date"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"
EARLIEST_DATE = "2020-01-01"


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

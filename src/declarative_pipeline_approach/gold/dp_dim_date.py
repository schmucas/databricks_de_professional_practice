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
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# --- config -----------------------------------------------------------------
TARGET_TABLE = "dp_dim_date"
EARLIEST_DATE = "2020-01-01"


# --- target -----------------------------------------------------------------
# TODO: materialized view named TARGET_TABLE

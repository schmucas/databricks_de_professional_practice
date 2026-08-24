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
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- config -----------------------------------------------------------------
SOURCE_TABLE = "dp_scd2_customers"
TARGET_TABLE = "dp_dim_customer"


# --- target -----------------------------------------------------------------
# TODO: materialized view named TARGET_TABLE
# TODO: surrogate key on (customer_id, start_at), match the classic hash exactly

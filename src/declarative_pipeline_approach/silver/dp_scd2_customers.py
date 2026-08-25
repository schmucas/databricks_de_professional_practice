"""
Silver | dp_scd2_customers | declarative track

Mirrors : src/classic_approach/silver/stream/silver_scd2_customers.ipynb
Source  : bronze.customers_raw  (Delta Change Data Feed, full-row post-images)
Target  : dp_scd2_customers  (SCD Type 2)

Pattern
  bronze CDF -> streaming source view -> create_streaming_table -> auto CDC flow (SCD 2)

Parity notes (what the classic foreachBatch MERGE does by hand)
  - drops _change_type = 'update_preimage'
  - orders changes inside a batch by _commit_version
  - closes the previous current row and opens a new one (start_at / end_at / is_current)
  - source deletes are flagged so gold can erase downstream (right-to-erasure path)

Declarative equivalents to reach for
  - sequence_by      -> the classic ordering key (_commit_version)
  - apply_as_deletes -> _change_type = 'delete'
  - except_column_list -> CDF metadata columns
  - expectations     -> customer_id NOT NULL

Sequencing, and why it is a struct
  The classic notebook uses two different columns for two different jobs: it
  ORDERS by _commit_version, but it STAMPS start_at from last_updated. Auto CDC
  has one knob for both, and whichever column it gets becomes __START_AT.
    - last_updated alone would give the right __START_AT value, but a CDF delete
      event carries the row's unchanged last_updated, so the delete would tie
      with the row it is meant to close and be discarded as out of order. The
      churn path would silently stop working.
    - _commit_version alone orders correctly but makes __START_AT a BIGINT,
      which destroys the validity window gold joins on.
  struct(last_updated, _commit_version) gets both: last_updated leads, so
  __START_AT.last_updated equals the classic start_at, and _commit_version breaks
  the tie so deletes are always sequenced after the row they close.

Column shape
  The auto CDC target keeps the native __START_AT / __END_AT and has no
  is_current column. See the paradigm note at the bottom for what that costs.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, TimestampType

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

# bronze is not the pipeline default schema, so it is qualified; silver is, so
# the target is named bare.
SOURCE_TABLE = f"{CATALOG}.bronze.customers_raw"
TARGET_TABLE = "dp_scd2_customers"

SOURCE_VIEW = "dp_customers_changes"


# --- source view ------------------------------------------------------------
@dp.temporary_view(name=SOURCE_VIEW)
def dp_customers_changes():
    """Cast the bronze customer change feed to the classic silver column shape.

    Drops CDF pre-images, exactly as the classic foreachBatch does before it
    builds its own ordering window.

    Returns:
        Streaming DataFrame of customer change events carrying the business
        columns, the `_change_type` / `_commit_version` CDF metadata the flow
        sequences and deletes on, and `_insert_update_ts`.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        .filter(F.col("_change_type") != "update_preimage")
        .withColumn("postal_code", F.col("postal_code").cast(IntegerType()))
        .withColumn("signup_date", F.col("signup_date").cast(DateType()))
        .withColumn("last_updated", F.col("last_updated").cast(TimestampType()))
        .withColumn("_insert_update_ts", F.current_timestamp())
        .select(
            "customer_id",
            "company_name",
            "contact_email",
            "address",
            "city",
            "postal_code",
            "tier",
            "industry",
            "signup_date",
            "_insert_update_ts",
            "_commit_version",
            "last_updated",
            "_change_type",
        )
    )


# --- target -----------------------------------------------------------------
dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Customer dimension history, SCD Type 2, built from the bronze CDF feed.",
    cluster_by=["_insert_update_ts", "customer_id"],
    expect_all_or_drop={"valid_customer_id": "customer_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target=TARGET_TABLE,
    source=SOURCE_VIEW,
    keys=["customer_id"],
    sequence_by=F.struct("last_updated", "_commit_version"),
    stored_as_scd_type=2,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    # No except_column_list for now. Excluding _change_type is the documented
    # pattern, but excluding last_updated is not: it is the first field of the
    # sequencing struct, and the docs never say whether a struct field's source
    # column may be dropped from the target. Until that is confirmed on a real
    # run, the target carries both columns. Cost: _change_type and last_updated
    # are two columns the classic table does not have, and last_updated is a
    # duplicate of __START_AT.last_updated.
)


# Paradigm note
#   Three MERGE statements, a lead() window and a hand-rolled _commit_version
#   idempotency guard collapse into one flow declaration, and the delete branch
#   the classic track needed a fourth statement for is a single parameter.
#   The cost is naming and shape: __START_AT / __END_AT are the pipeline's
#   columns, not ours, __END_AT is exclusive where the classic end_at is
#   inclusive (next_start minus 1 ms), and there is no is_current or _is_delete
#   flag, only "__END_AT IS NULL" as the current-row predicate.

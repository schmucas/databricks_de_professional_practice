"""
Gold | dp_fact_order_fulfillment | declarative track

Mirrors : src/classic_approach/gold/gold_fact_order_fulfillment.ipynb
Sources : dp_scd2_orders (silver), dp_dim_customer, dp_dim_location, dp_dim_date
Target  : dp_fact_order_fulfillment
Grain   : one row per order  (PK order_id)

This is the hardest table in the track. Two things happen at once.

1. Status pivot.  Silver holds one SCD2 row per status change. Gold needs one
   row per order with a timestamp column per status. The classic notebook does
   this by projecting a <status>_ts column for each value in STATUS_LIST, then
   taking last(ignorenulls) over an unbounded window partitioned by order_id
   ordered by start_at, then dropDuplicates on order_id.
   STATUS_LIST = picked_up, created, out_for_delivery, delivered,
                 failed_delivery, in_transit, returned, confirmed

2. SCD2-correct dimension lookup.  customer_sk is resolved as-of the order:
        orders.start_at >= dim_customer.start_at
    AND orders.start_at <  coalesce(dim_customer.end_at, <far future>)
   All four dimension joins are LEFT joins, a missing dimension row must not
   drop the fact.

Foreign keys
  customer_sk, orig_location_sk (origin_city), dest_location_sk (destination_city),
  order_date_key (order_date), delivery_date_key (delivered_ts cast to date)

Measures and flags
  quantity, total_amount, payment_method, current_status,
  confirmed_ts, picked_up_ts, in_transit_ts, out_for_delivery_ts, delivered_ts,
  order_date, delivery_date,
  delivery_days = datediff(delivery_date, order_date),
  is_on_time    = delivery_days <= 30
  plus product_code and product_name

Deletes: the classic track needs a separate notebook to erase rows for deleted
customers. On this track the silver auto CDC flow already applies the delete,
so the fact simply stops seeing that customer on refresh. Do not port
gold_delete_customer_downstream.ipynb, that is the point of the comparison.

Translating start_at to this track
  Orders sequence by _change_ts, a plain timestamp, so orders.__START_AT IS the
  classic orders.start_at, same type and same value.
  Customers sequence by struct(last_updated, _commit_version), so the customer
  side of the as-of predicate is __START_AT.last_updated, reached with
  .getField() rather than dotted string paths so the alias qualifier stays
  unambiguous.
  One semantic nicety: the classic end_at is INCLUSIVE (next_start minus 1 ms)
  and is compared with a strict <, while __END_AT is EXCLUSIVE. Strict < against
  an exclusive bound is the canonical form, so the two agree everywhere except
  inside that 1 ms sliver.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

# --- config -----------------------------------------------------------------
ENV = spark.conf.get("env")
CATALOG = f"sl_{ENV}"

SOURCE_ORDERS = "dp_scd2_orders"
DIM_CUSTOMER = "dp_dim_customer"
DIM_CUSTOMER_FQN = f"{CATALOG}.gold.{DIM_CUSTOMER}"
DIM_LOCATION = "dp_dim_location"
DIM_LOCATION_FQN = f"{CATALOG}.gold.{DIM_LOCATION}"
DIM_DATE = "dp_dim_date"
DIM_DATE_FQN = f"{CATALOG}.gold.{DIM_DATE}"
TARGET_TABLE = "dp_fact_order_fulfillment"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"

PIVOT_VIEW = "dp_order_status_pivot"

STATUS_LIST = [
    "picked_up",
    "created",
    "out_for_delivery",
    "delivered",
    "failed_delivery",
    "in_transit",
    "returned",
    "confirmed",
]


# --- status pivot -----------------------------------------------------------
@dp.temporary_view(name=PIVOT_VIEW)
def dp_order_status_pivot():
    """Collapse the SCD2 order history into one row per order.

    Projects a `<status>_ts` column per status, then carries the last non-null
    value of every column forward over an unbounded window ordered by
    `__START_AT`, so each order ends up holding its latest business values
    alongside the timestamp at which it first reached each status.

    Returns:
        Batch DataFrame with one row per order_id.
    """
    orders_df = spark.read.table(SOURCE_ORDERS)

    enriched_orders = orders_df.select(
        *orders_df.columns,
        *[
            F.when(F.col("status") == s, F.col("__START_AT")).otherwise(F.lit(None)).alias(f"{s}_ts")
            for s in STATUS_LIST
        ],
    )

    window_spec = (
        Window.partitionBy("order_id")
        .orderBy("__START_AT")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    cols = [c for c in enriched_orders.columns if c not in ("order_id")]

    return (
        enriched_orders.select(
            "order_id",
            *[F.last(F.col(c), ignorenulls=True).over(window_spec).alias(c) for c in cols],
        )
        .dropDuplicates(["order_id"])
        .withColumn("delivered_date", F.col("delivered_ts").cast(DateType()))
    )


# --- target -----------------------------------------------------------------
@dp.materialized_view(
    name=TARGET_FQN,
    comment="Order fulfillment fact, one row per order, with as-of customer and conformed dimensions.",
    cluster_by_auto=True,
    schema=f"""
        order_id STRING NOT NULL,
        customer_sk STRING,
        orig_location_sk STRING,
        dest_location_sk STRING,
        order_date_key INT,
        delivery_date_key INT,
        product_code STRING,
        product_name STRING,
        quantity INT,
        total_amount DOUBLE,
        payment_method STRING,
        current_status STRING,
        confirmed_ts TIMESTAMP,
        picked_up_ts TIMESTAMP,
        in_transit_ts TIMESTAMP,
        out_for_delivery_ts TIMESTAMP,
        delivered_ts TIMESTAMP,
        order_date DATE,
        delivery_date DATE,
        delivery_days INT,
        is_on_time BOOLEAN,
        CONSTRAINT dp_order_id_pk PRIMARY KEY (order_id),
        CONSTRAINT dp_fct_ordr_customer_sk_fk FOREIGN KEY (customer_sk)
            REFERENCES {DIM_CUSTOMER_FQN}(customer_sk),
        CONSTRAINT dp_fct_ordr_orig_location_sk_fk FOREIGN KEY (orig_location_sk)
            REFERENCES {DIM_LOCATION_FQN}(location_sk),
        CONSTRAINT dp_fct_ordr_dest_location_sk_fk FOREIGN KEY (dest_location_sk)
            REFERENCES {DIM_LOCATION_FQN}(location_sk),
        CONSTRAINT dp_fct_ordr_order_date_key_fk FOREIGN KEY (order_date_key)
            REFERENCES {DIM_DATE_FQN}(date_key),
        CONSTRAINT dp_fct_ordr_delivery_date_key_fk FOREIGN KEY (delivery_date_key)
            REFERENCES {DIM_DATE_FQN}(date_key)
    """,
)
def dp_fact_order_fulfillment():
    """Join the pivoted orders to the customer, location and date dimensions.

    customer_sk is resolved as-of the order's own `__START_AT` against the SCD2
    validity window of the customer dimension. All four joins are LEFT so a
    missing dimension row never drops a fact.

    Returns:
        Batch DataFrame with one row per order_id carrying the four surrogate
        keys, the status timestamps, the measures and the delivery flags.
    """
    pivoted_orders = spark.read.table(PIVOT_VIEW)
    dim_customers_df = spark.read.table(DIM_CUSTOMER_FQN)
    # Read the location and date dimensions twice rather than aliasing one
    # DataFrame into two, so the origin/destination and order/delivery joins
    # cannot collide on self-join ambiguity.
    dim_origin_df = spark.read.table(DIM_LOCATION_FQN)
    dim_destination_df = spark.read.table(DIM_LOCATION_FQN)
    dim_order_date_df = spark.read.table(DIM_DATE_FQN)
    dim_delivery_date_df = spark.read.table(DIM_DATE_FQN)

    customer_start_at = F.col("c.__START_AT").getField("last_updated")
    customer_end_at = F.col("c.__END_AT").getField("last_updated")
    far_future = F.date_add(F.current_date(), 1)

    return (
        pivoted_orders.alias("main")
        .join(
            dim_customers_df.alias("c"),
            on=(
                (F.col("main.customer_id") == F.col("c.customer_id"))
                & (F.col("main.__START_AT") >= customer_start_at)
                & (F.col("main.__START_AT") < F.coalesce(customer_end_at, far_future))
            ),
            how="left",
        )
        .join(dim_origin_df.alias("lo"), on=(F.col("main.origin_city") == F.col("lo.city")), how="left")
        .join(dim_destination_df.alias("ld"), on=(F.col("main.destination_city") == F.col("ld.city")), how="left")
        .join(dim_order_date_df.alias("od"), on=(F.col("main.order_date") == F.col("od.full_date")), how="left")
        .join(
            dim_delivery_date_df.alias("dd"),
            on=(F.col("main.delivered_ts").cast(DateType()) == F.col("dd.full_date")),
            how="left",
        )
        .select(
            "order_id",
            "c.customer_sk",
            F.col("lo.location_sk").alias("orig_location_sk"),
            F.col("ld.location_sk").alias("dest_location_sk"),
            F.col("od.date_key").alias("order_date_key"),
            F.col("dd.date_key").alias("delivery_date_key"),
            "main.product_code",
            "main.product_name",
            "main.quantity",
            "main.total_amount",
            "main.payment_method",
            F.col("main.status").alias("current_status"),
            "main.confirmed_ts",
            "main.picked_up_ts",
            "main.in_transit_ts",
            "main.out_for_delivery_ts",
            "main.delivered_ts",
            "main.order_date",
            "main.delivery_date",
        )
        .withColumn("delivery_days", F.datediff(F.col("delivery_date"), F.col("order_date")))
        .withColumn("is_on_time", F.when(F.col("delivery_days") <= 30, F.lit(True)).otherwise(F.lit(False)))
    )


# Paradigm note
#   The pivot is identical on both tracks, because it is ordinary Spark and
#   declarative has nothing to add to a window function. Everything around it
#   changes: classic reads a date window, joins that back to the full silver
#   table to recover each touched order's whole history, then MERGEs on order_id,
#   and needs gold_delete_customer_downstream.ipynb afterwards to chase deleted
#   customers out. Here the fact is simply defined as a function of silver, so
#   the erasure notebook has nothing to do and does not exist.
#   The cost is the usual one: a full recompute of 50k+ orders per refresh where
#   classic touches only the orders that moved.

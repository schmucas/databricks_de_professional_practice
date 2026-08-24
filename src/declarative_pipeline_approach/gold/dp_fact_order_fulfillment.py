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
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- config -----------------------------------------------------------------
SOURCE_ORDERS = "dp_scd2_orders"
DIM_CUSTOMER = "dp_dim_customer"
DIM_LOCATION = "dp_dim_location"
DIM_DATE = "dp_dim_date"
TARGET_TABLE = "dp_fact_order_fulfillment"

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
# TODO: temporary view, one row per order with a <status>_ts column per status.


# --- target -----------------------------------------------------------------
# TODO: materialized view named TARGET_TABLE, joining the pivot to the dimensions

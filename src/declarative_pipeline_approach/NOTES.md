# Declarative pipeline track — notes

Context and design notes for `src/declarative_pipeline_approach/`, moved out of
file comments and summarized here. Each table mirrors an equivalent notebook in
`src/classic_approach/` — the two tracks compute the same output through
different paradigms (declarative pipelines vs. hand-written batch/streaming
notebooks), as a comparison exercise. Function-level Google-style docstrings
(Args/Returns) stay in the code; everything else lived in comments and is
summarized here per table.

## Silver

### dp_scd1_vehicles
- Mirrors `silver/batch/silver_scd1_vehicles.ipynb`. Source: `bronze.vehicles_raw` (Auto Loader). SCD Type 1, current state only, no history/deletes/backfill.
- Filters `vehicle_id IS NULL`; dedupes to the latest row per `vehicle_id` by `last_updated` (descending).
- Classic hand-writes a dedupe window + MERGE; here the dedupe collapses into one `create_auto_cdc_flow` (keys + `sequence_by`), and it stays correct across batches where the classic window only sees the rows in front of it. The classic NULL filter becomes a published expectation instead of a silent `.filter()`.

### dp_scd2_customers
- Mirrors `silver/stream/silver_scd2_customers.ipynb`. Source: `bronze.customers_raw` (Delta CDF, full-row post-images). SCD Type 2.
- Pattern: bronze CDF → streaming source view → `create_streaming_table` → auto CDC flow (SCD 2). Drops `_change_type = 'update_preimage'`; deletes flagged via `apply_as_deletes` for the right-to-erasure path.
- Sequencing is a struct, not a single column, because the classic notebook uses two different columns for two jobs: it orders by `_commit_version` but stamps `start_at` from `last_updated`. `last_updated` alone would tie a CDF delete event with the row it should close (delete silently lost); `_commit_version` alone would make `__START_AT` a BIGINT and break the validity-window joins. `struct(last_updated, _commit_version)` gets both — `last_updated` leads (so `__START_AT.last_updated` equals the classic `start_at`) and `_commit_version` breaks ties so deletes always sequence after the row they close.
- Cost: the auto CDC target keeps native `__START_AT`/`__END_AT` with no `is_current` column — only `__END_AT IS NULL` as the current-row predicate. `__END_AT` is exclusive where classic `end_at` is inclusive (next start minus 1 ms). No `except_column_list` yet: excluding `_change_type` is documented, but whether a struct field's source column (`last_updated`) can be excluded is unconfirmed, so both `_change_type` and `last_updated` remain on the target (the latter duplicates `__START_AT.last_updated`).

### dp_scd2_orders
- Mirrors `silver/stream/silver_scd2_orders.ipynb`. Source: `bronze.orders_raw` (file-based CDC export, Auto Loader). SCD Type 2, order-status history.
- Sequences by `_change_ts` (not a Delta commit version — source is file-based). No deletes on this source. ~2% of rows arrive with missing `total_amount`; kept, not dropped.
- Contrast with `dp_scd2_customers`: same SCD2 outcome, different change-capture mechanism (CDF vs. file-based CDC). No struct needed here because `_change_ts` already does both jobs (ordering and validity stamping) that customers needed two columns for, so `__START_AT` is a plain timestamp equal to the classic `start_at`.
- The missing `total_amount` gap exists on both tracks today, but only this one could turn it into a published warn expectation later without touching the data.

### dp_shipment_events
- Mirrors `silver/batch/silver_shipment_events.ipynb`. Source: `bronze.shipment_events_raw` (high-volume event stream, Auto Loader). Append-only, deduplicated.
- ~3% duplicate `event_id`s; classic dedupes ascending (keeps earliest `event_timestamp`). Auto CDC always keeps the *highest* `sequence_by` value, so the flow sequences by a negated event time (`_dedupe_seq` = `-unix_micros(event_timestamp)`), turning "highest wins" into "earliest wins"; that helper column is dropped from the target via `except_column_list`.
- Classic's dedupe only sees one batch — a duplicate landing in a later run is invisible to the window and gets merged on top. Auto CDC keys on `event_id` for the life of the table, so the dedupe holds across batches too.

### dp_static_location_lookup
- Mirrors `silver/static/silver_static_location_lookup.ipynb`. No bronze parent — hardcoded seed list, values copied verbatim from classic (26 Middle-earth cities). Exists so `dp_dim_location` can enrich raw city strings from orders.
- Seeded via a one-shot `append_flow(once=True)`, which the pipeline itself remembers having run — the declarative equivalent of the classic `INITIAL_RUN` flag a human has to remember to unset. Trade-off: editing the seed list has no effect until a full refresh, where classic only needs the flag flipped.

### dp_vehicle_telemetry
- Mirrors `silver/batch/silver_vehicle_telemetry.ipynb` (+ `silver_vehicle_telemetry_transforms.py`). Source: `bronze.vehicle_telemetry_raw` (IoT, at-least-once delivery, Auto Loader). Append-only, deduplicated, enriched.
- Normalize: dedupe key is `reading_id` (timestamps not unique); out-of-range sensor values become NULL (`cargo_temp_c` outside [-273,100], `engine_temp_c` outside [0,200], `speed_kmh` outside [0,180]). Enrich: `is_cold_chain_cargo`, `speed_limit_exceed` (>120), `engine_overheat` (>110), `reading_date`.
- Classic does the range checks in Python and just keeps the row silently corrected. Here they're expressed as expectations (`SENSOR_RANGE_EXPECTATIONS`) on `dp_vehicle_telemetry_cast` — a *separate* view from the one that nulls the values out, so the checks run **before** nulling. Nulling first would make the expectations unfalsifiable (100% pass on data classic knows is bad). NULL passes the expectation deliberately — ~70% of readings have no `cargo_temp_c` at all and that's expected, not a fault.
- Same ascending-dedupe pattern as `dp_shipment_events` (negated `_dedupe_seq`).
- What declarative doesn't give back: the classic notebook's skew hint for the gold aggregate (see `dp_fact_vehicle_telemetry`) — the pipeline owns the plan, not us.

## Gold

### dp_dim_customer
- Mirrors `gold/gold_dim_customer.ipynb`. Source: `dp_scd2_customers`. SCD Type 2 dimension, `customer_sk` = MD5(customer_id, start_at). Excludes rows flagged deleted at silver. Since silver already historizes the entity, gold is a projection + surrogate key → a materialized view suffices.
- `START_AT` here means `__START_AT.last_updated` specifically — hashing the whole struct would stringify it (`"{2025-01-01 00:00:00, 12}"`) and diverge from the classic `customer_sk` for every row, so the surrogate key still hits parity even though the SCD2 columns (`__START_AT`/`__END_AT`, no `is_current`, no delete flag) don't.
- The `__START_AT`/`__END_AT` schema types are tied to the silver flow's `sequence_by` shape (`struct(last_updated, _commit_version)`) — changing that sequence requires updating this schema string too.
- Paradigm: classic carries a `_is_delete` flag from silver and needs a separate `gold_delete_customer_downstream.ipynb` to chase already-written rows out of gold. Not needed here — silver's auto CDC flow applies the delete, and this view recomputes from silver every refresh, so gold can't hold a row silver no longer justifies. Note "applied the delete" under SCD2 only closes the churned customer's current row (`__END_AT` set); it does not erase history — correct for a Type 2 dimension, not sufficient for a right-to-erasure request (would need Type 1 or a UC purge).

### dp_dim_date
- Mirrors `gold/gold_dim_date.ipynb`. No upstream table — generated. Range: 2020-01-01 through today via `sequence()` + explode. `date_key` = `yyyyMMdd` INT (PK). Columns: date_key, full_date, year, month, month_name, iso_week, day_of_week, is_weekend (classic README also lists quarter/day_name/is_holiday but doesn't build them — parity kept, don't silently add them here).
- Generated tables are awkward in a declarative pipeline: a materialized view can't read itself, so unlike the classic notebook (which reads `max(full_date)` off its own target and generates only the gap since the last run), this always regenerates the whole range from `EARLIEST_DATE`. Same rows, no self-reference, but ~2,400 rows recomputed every refresh instead of the classic one-row-a-day increment.

### dp_dim_location
- Mirrors `gold/gold_dim_location.ipynb`. Sources: `dp_scd2_orders`, `dp_static_location_lookup`. Collects distinct cities from both `origin_city`/`destination_city`, joins the lookup on a normalized city string (broadcast, 26 rows), unmatched cities become `'unknown'`. `location_sk` = MD5(city).
- Classic once had a bug reading a stale lookup table name — this points at `dp_static_location_lookup` deliberately.
- Two behaviors reproduced on purpose (breaking either would break the A/B comparison): (1) the null/empty filter is a single AND across both columns, so a blank `destination_city` drops the row's contribution for *both* cities, not just the blank one (~1% of generated orders have blank destinations); (2) unmatched cities all collapse into one row — `city` is overwritten with `'unknown'` before `dropDuplicates`, so the original city name is lost and the fact's join on `city` resolves those orders to a NULL `location_sk` rather than the `'unknown'` row.
- Paradigm: this is the one table where declarative quietly *fixes* a real defect — classic reads only the orders in its date window, so a city that stops appearing in new orders keeps a stale row forever (and a mis-set window silently narrows the dimension); the materialized view is defined against all of silver, so the dimension is a pure function of the data, not of when the job last ran. What's *not* fixed: the union filter and collapsed-unknown-row logic bugs above — declarative has no opinion about logic bugs.

### dp_dim_vehicle
- Mirrors `gold/gold_dim_vehicle.ipynb`. Source: `dp_scd1_vehicles`. SCD Type 1, `vehicle_sk` = MD5(vehicle_id), current state only. Thinnest table in the track — silver already holds current state, gold just adds the surrogate key.
- Paradigm: identical output, opposite mechanics. Classic MERGEs whatever batch it happened to read into existing gold, so correctness depends on the job's window; the materialized view derives gold from all of silver every time, so a wrong `START_DATE` can't corrupt it. The surrogate key hash itself is the one thing declarative doesn't simplify — it has to be copied here since `%run` has no pipeline equivalent.

### dp_fact_order_fulfillment
- Mirrors `gold/gold_fact_order_fulfillment.ipynb`. Sources: `dp_scd2_orders`, `dp_dim_customer`, `dp_dim_location`, `dp_dim_date`. Grain: one row per order (PK `order_id`). The hardest table in the track — two things happen at once:
  1. **Status pivot** — silver holds one SCD2 row per status change; gold needs one row per order with a `<status>_ts` column per status (`STATUS_LIST`: picked_up, created, out_for_delivery, delivered, failed_delivery, in_transit, returned, confirmed). Built with `last(ignorenulls)` over an unbounded window partitioned by `order_id`, ordered by `start_at`, then `dropDuplicates` on `order_id`.
  2. **SCD2-correct dimension lookup** — `customer_sk` resolved as-of the order: `orders.start_at >= dim_customer.start_at AND orders.start_at < coalesce(dim_customer.end_at, <far future>)`. All four dimension joins are LEFT so a missing dimension row never drops the fact.
- Foreign keys: `customer_sk`, `orig_location_sk`/`dest_location_sk` (by city), `order_date_key`/`delivery_date_key` (by date). Measures/flags: quantity, total_amount, payment_method, current_status, the status timestamps, order/delivery dates, `delivery_days` (datediff), `is_on_time` (<=30 days), plus product_code/product_name.
- Deletes: classic needs a separate notebook to erase rows for deleted customers; here silver's auto CDC already applies the delete, so the fact just stops seeing that customer on refresh (`gold_delete_customer_downstream.ipynb` deliberately not ported).
- Translating `start_at`: orders sequence by `_change_ts` (a plain timestamp) so `orders.__START_AT` *is* the classic `orders.start_at`. Customers sequence by `struct(last_updated, _commit_version)`, so the customer side of the as-of predicate is `__START_AT.getField("last_updated")` (`.getField()` rather than dotted paths, to keep the alias qualifier unambiguous). Classic's `end_at` is inclusive (next_start minus 1ms) and compared with strict `<`; `__END_AT` is exclusive, so strict `<` against it is the canonical form — the two agree everywhere except a 1ms sliver.
- The two dimension reads for origin/destination and order/delivery are each read twice (not aliased from one DataFrame) so the self-joins can't collide on ambiguity.
- Paradigm: the pivot itself is identical on both tracks (ordinary Spark, nothing declarative adds). Everything around it changes — classic reads a date window, joins back to full silver to recover each touched order's whole history, MERGEs on `order_id`, then needs the delete-downstream notebook; here the fact is simply a function of silver, so that notebook has nothing to do. Cost: a full recompute of 50k+ orders per refresh vs. classic touching only the orders that moved.

### dp_fact_shipment_event
- Mirrors `gold/gold_fact_shipment_event.ipynb`. Sources: `dp_shipment_events`, `dp_dim_vehicle`, `dp_dim_date`. Grain: one row per tracking event (PK `event_id`). LEFT joins dim_vehicle (on vehicle_id) and dim_date (on event_date); `order_id` rides along as a degenerate dimension (no order FK here).
- Deliberately a **materialized view, not a streaming table**, despite silver being append-looking: `dp_shipment_events` is an auto CDC target, so it is NOT append-only — a duplicate `event_id` arriving later rewrites the row that already won. A streaming read would fail outright unless it set `skipChangeCommits=true`, and with that set the fact would keep the superseded row forever. The dimensions are also recomputed materialized views, so a streaming fact would pin stale surrogate keys too.
- Known parity gap (deliberate, not yet closed): classic aliases its pass-through columns as literally `"main.<name>"` (a typo for `F.col(f"main.{c}")`, which this table does correctly) — so the classic table's actual column names are things like `main.event_timestamp`. This track uses clean names. Until the classic notebook is patched, the two fact tables differ in column *naming* only, not values/types/order.
- Paradigm: "append-only fact → streaming table" is the wrong instinct once silver is an auto CDC target — the CDC layer rewrites rows, and a streaming reader either errors or has to be told to ignore exactly the corrections that make silver worth reading. Cost of the materialized view is real: classic MERGEs only its window's events, this recomputes the whole fact from silver every run.

### dp_fact_vehicle_telemetry
- Mirrors `gold/gold_fact_vehicle_telemetry.ipynb`. Sources: `dp_vehicle_telemetry`, `dp_dim_vehicle`, `dp_dim_date`. Grain: one row per vehicle per hour (PK `vehicle_id` + `period_start_timestamp`).
- Aggregation (verbatim from classic): `period_start_timestamp = date_trunc('hour', reading_timestamp)`, grouped by vehicle_id/period, with `readings_count`, `avg_speed_kmh` (rounded), `max_speed_kmh`, `min/max_odometer_km` (intermediate only), `avg_fuel_pct` (rounded 2dp), `avg_engine_temp_c` (rounded), `min/max_cargo_temp_c`, `vehicle_idle` = count of readings NOT in ('idle','maintenance'). Derived: `km_driven` = max−min odometer, `utilization_pct` = vehicle_idle/readings_count (rounded 2dp), `period_date` = date-cast period start; then the intermediate odometer/idle columns are dropped. LEFT joins dim_vehicle (vehicle_id) and dim_date (period_date).
- `vehicle_idle` is misleadingly named — it actually sums readings whose status is neither `idle` nor `maintenance`, i.e. it's the *active* reading count, and `utilization_pct` is the active share. Copied as-is including the misleading name, because renaming it would break the classic/declarative comparison.
- Skew: 60% of readings come from 5 of 200 vehicles. Classic carries an explicit `hint('skew', 'vehicle_id')`; the declarative version has no equivalent knob and leaves skew handling to serverless AQE, which usually — but not guaranteedly — handles it.
- Paradigm: the one table where declarative is strictly worse, worth saying plainly. Overall track summary: declarative wins wherever the work is bookkeeping (CDC, dedupe, dependency order, incremental windows) and loses wherever the work is performance engineering the pipeline doesn't expose a knob for.

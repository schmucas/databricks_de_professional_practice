# Setup: source data generation

Two notebooks build and maintain the Shadowfax Logistics source data that the ingestion
and pipeline tracks read from. Run `environment_setup` once, then run
`incremental_data_generator` before each pipeline execution to add fresh, messy data.

## Notebooks

### `environment_setup.ipynb` (one-time seed)

Creates the catalogs, schemas and landing volume, then writes the initial seed.
It is **destructive**: it drops `sl_ingest` and `sl_{env}` with `CASCADE` and
recreates everything, so re-running it wipes all downstream tables too.

It provisions:

- `sl_ingest` catalog: the raw source (Delta tables + a JSON landing volume).
- `sl_{env}` catalog: `bronze` / `silver` / `gold` schemas plus a `checkpoints` volume.

### `incremental_data_generator.ipynb` (run before each pipeline execution)

Appends a new batch to the same sources on every run. It can inject data-quality
problems and schema drift so the ingestion and transformation tracks always have
new, realistic mess to handle. Every run also appends one row per source to an
audit table (see below).

## Sources produced

| Source | Location | Format | Notes |
|---|---|---|---|
| `customers` | `sl_ingest.{env}.customers` | Delta, CDF on | Current-state source (simulates Postgres), one row per customer (named Middle-earth characters); CDC feeds the warehouse SCD2 build |
| `orders` | `/Volumes/sl_ingest/{env}/landing/orders` | JSON files | File-based CDC feed: SNAPSHOT seed (50K full rows, ~2% NULL amounts), then INSERT (full rows) and sparse UPDATE drops; no deletes |
| `shipment_events` | `/Volumes/sl_ingest/{env}/landing/shipment_events` | JSON files | High volume, dupes / late / bad records |
| `telemetry` | `/Volumes/sl_ingest/{env}/landing/telemetry` | JSON files | Skewed 60% to 5 vehicles |
| `vehicles` | `/Volumes/sl_ingest/{env}/landing/vehicles` | JSON files | Fleet master (200 pony carts, wains and cold wagons); one-time seed plus rare full-row depot reassignments (SCD1) |

## How updates work

`customers` is the only Delta source; it has Change Data Feed enabled, so the
bronze loader reads incremental changes via `readChangeFeed`. All other sources
(including `orders`) are JSON feeds on the landing volume: each run lands new
files for Auto Loader to pick up.

**Customers (simulated Postgres, SCD2 source).** `sl_ingest.customers` stands in for
an operational **Postgres** customers table: current state only, one row per customer,
no SCD2 columns. Each batch mutates it the way an application would, and CDF turns
those mutations into the change events the warehouse uses to build a Type 2 dimension:

- **Update**: a subset of existing customers change tier and address/city in place
  (`last_updated` set to the batch time). CDF emits `update_preimage` / `update_postimage`.
- **Delete**: a few customers churn and are removed. CDF emits `delete`.
- **Insert**: brand-new customers continue the id sequence. CDF emits `insert`.
- **Unchanged**: everyone not picked is left alone, so silver has to ignore no-ops.

The SCD2 history (`valid_from` / `valid_to` / `is_current`) is **not** stored in the
source. Silver reconstructs it from the bronze CDF log using `_change_type` and commit
order, the standard CDC-to-SCD2 pattern.

**Orders (file-based CDC, SCD2 source).** Orders arrive as a **file-based CDC feed**
(a vendor-extract / Debezium-style drop), not a Delta table. The seed writes an initial
**SNAPSHOT**: one full row per order with `_change_type = "SNAPSHOT"` (analogous to
Debezium's read op), sequenced by the order's own `order_date`. Each generator batch
then appends a change drop to the same path: brand-new orders as **INSERT** (full rows)
and status changes as **UPDATE** rows that are **sparse**, key plus changed columns only,
since Spark's JSON writer drops null fields per row. No deletes in this feed by design.
`_change_ts` (batch time) is the sequence column, so every change sorts after the
snapshot. Silver must first rebuild each full post-image from the current version,
then apply SCD2, the deliberate contrast to the customers CDF path.

## Controls (job parameters)

| Parameter | Default | Effect |
|---|---|---|
| `env` | `dev` | Target catalog/schema (`sl_ingest.{env}`, `sl_{env}`) |
| `n_orders`, `n_customers`, `n_events`, `n_telemetry` | varies | Batch sizes per source |
| `n_cdc` | `275` | Sparse status UPDATEs appended to the orders feed per batch |
| `n_vehicle_updates` | `2` | Fleet-master depot reassignments per batch (full-row post-images, SCD1); `0` disables |
| `inject_anomalies` | `true` | Toggle bad / duplicate / late records |
| `drift_this_run` | empty | Comma-separated JSON sources to drift this run (e.g. `telemetry,shipment_events`) |

Anomaly types per source are set in the `DATASETS` map. Schema drift is JSON-only and
deterministic: each listed source gains its next candidate column, which then persists
on all later runs (state is rebuilt from the audit table).

## Audit / run-tracking

Every generator run appends one row per source to
`sl_ingest.{env}._incremental_generator_runs`: rows added, bad / duplicate / late
counts, drift action, cumulative active extra columns, and a classification. It is
append-only history and also the source of the persistent drift state.

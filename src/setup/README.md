# Setup: source data generation

Two notebooks build and maintain the SwissLogistics source data that the ingestion
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
| `customers` | `sl_ingest.{env}.customers` | Delta, CDF on | Versioned SCD2 source, one `is_current=true` row per customer |
| `orders` | `sl_ingest.{env}.orders` | Delta, CDF on | Order lifecycle, ~2-3% data-quality issues |
| `shipment_events` | `/Volumes/sl_ingest/{env}/landing/shipment_events` | JSON files | High volume, dupes / late / bad records |
| `telemetry` | `/Volumes/sl_ingest/{env}/landing/telemetry` | JSON files | Skewed 60% to 5 vehicles |
| `cdc_records` | `/Volumes/sl_ingest/{env}/landing/cdc_records` | JSON files | Order change feed for MERGE / upsert practice |

## How updates work

The Delta sources (`customers`, `orders`) have Change Data Feed enabled, so the
bronze loaders read incremental changes via `readChangeFeed`. The JSON sources land
new files each run for Auto Loader to pick up.

**Customers (SCD2).** Each incremental batch uses the live current customers as the
reference and produces genuine changes, not random near-duplicates:

- **Attribute updates**: a subset get a new tier and address/city. The new version is
  appended (`is_current=true`, `changed_at=null`) and the prior current row is
  retired (`is_current=false`, `changed_at=<batch ts>`).
- **Churn (soft-delete)**: a few current rows are retired with no replacement, so no
  current version remains. Soft-delete is signalled purely via `is_current`.
- **New customers**: brand-new ids continue the sequence.
- **Unchanged**: everyone not picked is left alone, so silver has to ignore no-ops.

The retirements are real Delta `UPDATE`s, so the source CDF emits
`update_preimage` / `update_postimage` for the flips and `insert` for the appended
rows. The SCD2 invariant (exactly one current row per customer) always holds.

## Controls (job parameters)

| Parameter | Default | Effect |
|---|---|---|
| `env` | `dev` | Target catalog/schema (`sl_ingest.{env}`, `sl_{env}`) |
| `n_orders`, `n_customers`, `n_events`, `n_telemetry`, `n_cdc` | varies | Batch sizes per source |
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

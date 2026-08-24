"""
Gold | dp_fact_vehicle_telemetry | declarative track

Mirrors : src/classic_approach/gold/gold_fact_vehicle_telemetry.ipynb
Sources : dp_vehicle_telemetry (silver), dp_dim_vehicle, dp_dim_date
Target  : dp_fact_vehicle_telemetry
Grain   : one row per vehicle per hour  (PK vehicle_id + period_start_timestamp)

Aggregation, verbatim from the classic notebook
  period_start_timestamp = date_trunc('hour', reading_timestamp)
  group by vehicle_id, period_start_timestamp:
    readings_count    = count(reading_id)
    avg_speed_kmh     = round(avg(speed_kmh))
    max_speed_kmh     = max(speed_kmh)
    min_odometer_km   = min(odometer_km)      -- intermediate only
    max_odometer_km   = max(odometer_km)      -- intermediate only
    avg_fuel_pct      = round(avg(fuel_pct), 2)
    avg_engine_temp_c = round(avg(engine_temp_c))
    min_cargo_temp_c  = min(cargo_temp_c)
    max_cargo_temp_c  = max(cargo_temp_c)
    vehicle_idle      = sum(vehicle_status NOT IN ('idle','maintenance'))

Derived
  km_driven       = max_odometer_km - min_odometer_km
  utilization_pct = round(vehicle_idle / readings_count, 2)
  period_date     = period_start_timestamp cast to date
  then drop min_odometer_km, max_odometer_km, vehicle_idle

Then LEFT join dim_vehicle on vehicle_id and dim_date on period_date.

Skew: 60 percent of readings come from 5 vehicles. The classic notebook
carries an explicit hint('skew', 'vehicle_id'). The declarative version
does not get to hand-tune that, which is a real trade-off and a good
answer to "when would you not use declarative pipelines".
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# --- config -----------------------------------------------------------------
SOURCE_TELEMETRY = "dp_vehicle_telemetry"
DIM_VEHICLE = "dp_dim_vehicle"
DIM_DATE = "dp_dim_date"
TARGET_TABLE = "dp_fact_vehicle_telemetry"


# --- hourly aggregate -------------------------------------------------------
# TODO: temporary view with the aggregation above.


# --- target -----------------------------------------------------------------
# TODO: materialized view named TARGET_TABLE, joined to the dimensions

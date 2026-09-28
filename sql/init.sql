CREATE TABLE IF NOT EXISTS meter_readings(event_id uuid PRIMARY KEY, meter_id text NOT NULL, household_id text NOT NULL, grid_zone text NOT NULL, power_consumption_kwh double precision NOT NULL CHECK(power_consumption_kwh>=0), solar_generation_kwh double precision NOT NULL CHECK(solar_generation_kwh>=0), net_grid_kwh double precision NOT NULL CHECK(net_grid_kwh>=0), event_time timestamptz NOT NULL);
CREATE INDEX IF NOT EXISTS idx_meter_time_zone ON meter_readings(event_time DESC, grid_zone);
CREATE TABLE IF NOT EXISTS zone_metrics(id bigserial PRIMARY KEY, window_start timestamptz, window_end timestamptz, grid_zone text, consumption_kwh double precision, solar_kwh double precision, net_grid_kwh double precision, active_households integer, renewable_pct double precision);
CREATE INDEX IF NOT EXISTS idx_zone_metrics_latest ON zone_metrics(grid_zone,window_start DESC);
CREATE TABLE IF NOT EXISTS tariffs(household_id text PRIMARY KEY, tariff_rate numeric(8,4), billing_tier text, subsidy_flag boolean, effective_date date);
CREATE TABLE IF NOT EXISTS daily_billing(report_date date, household_id text, grid_zone text, consumption_kwh double precision, solar_kwh double precision, net_grid_kwh double precision, tariff_rate numeric(8,4), billing_tier text, subsidy_flag boolean, estimated_bill numeric(12,2), PRIMARY KEY(report_date,household_id));


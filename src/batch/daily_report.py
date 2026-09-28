import csv
from datetime import date
from pathlib import Path
import psycopg2
from src.common.config import POSTGRES
from src.common.logging import configure

log = configure("daily-report")
SQL = """
INSERT INTO daily_billing(report_date, household_id, grid_zone, consumption_kwh, solar_kwh, net_grid_kwh, tariff_rate, billing_tier, subsidy_flag, estimated_bill)
SELECT %s, m.household_id, max(m.grid_zone), sum(m.power_consumption_kwh), sum(m.solar_generation_kwh), sum(m.net_grid_kwh),
       t.tariff_rate, t.billing_tier, t.subsidy_flag, round((sum(m.net_grid_kwh) * t.tariff_rate)::numeric, 2)
FROM meter_readings m JOIN tariffs t ON t.household_id=m.household_id
WHERE m.event_time >= %s::date AND m.event_time < %s::date + interval '1 day'
GROUP BY m.household_id,t.tariff_rate,t.billing_tier,t.subsidy_flag
ON CONFLICT (report_date, household_id) DO UPDATE SET consumption_kwh=excluded.consumption_kwh, solar_kwh=excluded.solar_kwh, net_grid_kwh=excluded.net_grid_kwh, tariff_rate=excluded.tariff_rate, estimated_bill=excluded.estimated_bill;
"""

def load_tariffs(path):
    with psycopg2.connect(**POSTGRES) as conn, conn.cursor() as cur, open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cur.execute("INSERT INTO tariffs(household_id,tariff_rate,billing_tier,subsidy_flag,effective_date) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(household_id) DO UPDATE SET tariff_rate=excluded.tariff_rate,billing_tier=excluded.billing_tier,subsidy_flag=excluded.subsidy_flag,effective_date=excluded.effective_date", (row["household_id"],row["tariff_rate"],row["billing_tier"],row["subsidy_flag"].lower()=="true",row["effective_date"]))

def create_report(report_date: date, output_dir="data/reports"):
    with psycopg2.connect(**POSTGRES) as conn, conn.cursor() as cur:
        cur.execute(SQL,(report_date,report_date,report_date)); cur.execute("SELECT * FROM daily_billing WHERE report_date=%s ORDER BY household_id",(report_date,)); rows=cur.fetchall(); headers=[d.name for d in cur.description]
    target=Path(output_dir); target.mkdir(parents=True,exist_ok=True); path=target/f"daily_billing_{report_date.isoformat()}.csv"
    with path.open("w",newline="",encoding="utf-8") as f: w=csv.writer(f); w.writerow(headers); w.writerows(rows)
    log.info("daily_report_created",extra={"path":str(path),"rows":len(rows)}); return path


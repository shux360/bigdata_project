from datetime import datetime, timezone, date
import csv
from src.producers.meter_producer import build_reading
from src.producers.tariff_batch import generate_tariffs

def test_meter_reading_contract():
    event=build_reading(1,datetime(2026,1,1,12,tzinfo=timezone.utc))
    assert event["household_id"]=="H001" and event["grid_zone"]=="south"
    assert event["power_consumption_kwh"]>=0 and event["solar_generation_kwh"]>=0

def test_tariff_file_has_all_households(tmp_path):
    path=generate_tariffs(date(2026,1,1),tmp_path)
    with path.open() as f: rows=list(csv.DictReader(f))
    assert len(rows)==20 and {r["billing_tier"] for r in rows}=={"lifeline","standard","peak"}


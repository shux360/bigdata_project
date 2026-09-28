import json, random, time, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from src.common.config import KAFKA
from src.common.logging import configure

log = configure("meter-producer")
ZONES = ["north", "south", "east", "west"]

def build_reading(index: int, now=None) -> dict:
    now = now or datetime.now(timezone.utc)
    consumption = round(random.uniform(0.25, 4.5), 3)
    daylight = 6 <= now.hour <= 18
    solar = round(random.uniform(0, 3.0) if daylight else 0.0, 3)
    return {"event_id": str(uuid.uuid4()), "meter_id": f"M{index:03d}",
            "household_id": f"H{index:03d}", "power_consumption_kwh": consumption,
            "solar_generation_kwh": solar, "grid_zone": ZONES[index % len(ZONES)],
            "timestamp": now.isoformat()}

def main():
    producer = Producer({"bootstrap.servers": KAFKA, "client.id": "meter-simulator", "enable.idempotence": True})
    while True:
        for i in range(1, 21):
            event = build_reading(i)
            producer.produce("meter-readings", key=event["household_id"], value=json.dumps(event))
            log.info("meter_event_sent", extra={"event_id": event["event_id"], "household_id": event["household_id"]})
        producer.flush(5)
        time.sleep(3)

if __name__ == "__main__": main()


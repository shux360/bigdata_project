import csv, json, os, random
from datetime import date
from pathlib import Path
from confluent_kafka import Producer
from src.common.config import KAFKA
from src.common.logging import configure

log = configure("tariff-batch")

def generate_tariffs(run_date: date, output_dir="data/batch") -> Path:
    path = Path(output_dir); path.mkdir(parents=True, exist_ok=True)
    target = path / f"tariffs_{run_date.isoformat()}.csv"
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["household_id", "tariff_rate", "billing_tier", "subsidy_flag", "effective_date"])
        writer.writeheader()
        for i in range(1, 21):
            tier = "lifeline" if i % 7 == 0 else ("peak" if i % 3 == 0 else "standard")
            writer.writerow({"household_id": f"H{i:03d}", "tariff_rate": {"lifeline": .12, "standard": .22, "peak": .31}[tier], "billing_tier": tier, "subsidy_flag": tier == "lifeline", "effective_date": run_date.isoformat()})
    return target

def publish_file(path: Path) -> int:
    producer = Producer({"bootstrap.servers": KAFKA, "client.id": "tariff-loader", "enable.idempotence": True})
    count = 0
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            row["tariff_rate"] = float(row["tariff_rate"]); row["subsidy_flag"] = row["subsidy_flag"].lower() == "true"
            producer.produce("tariff-updates", key=row["household_id"], value=json.dumps(row)); count += 1
    producer.flush(10); log.info("tariff_file_published", extra={"path": str(path), "records": count})
    return count

if __name__ == "__main__": publish_file(generate_tariffs(date.today(), os.getenv("BATCH_OUTPUT_DIR", "data/batch")))


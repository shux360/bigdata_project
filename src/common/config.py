import os

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "smartgrid"),
    "user": os.getenv("POSTGRES_USER", "smartgrid"),
    "password": os.getenv("POSTGRES_PASSWORD", "smartgrid"),
}


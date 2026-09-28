import json
import logging
import sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {"level": record.levelname, "component": record.name, "message": record.getMessage()}
        for key in ("event_id", "household_id", "path", "records", "rows"):
            if hasattr(record, key): payload[key] = getattr(record, key)
        return json.dumps(payload, default=str)

def configure(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

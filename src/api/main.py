import time
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import psycopg2
from psycopg2.extras import RealDictCursor
from src.common.config import POSTGRES

app = FastAPI(title="Smart Grid Serving API", version="1.0.0")
REQUESTS=Counter("api_requests_total","API requests",["path","status"]); LATENCY=Histogram("api_request_seconds","API latency",["path"])

def query(sql,args=()):
    with psycopg2.connect(**POSTGRES) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur: cur.execute(sql,args); return cur.fetchall()

@app.get("/health")
def health():
    try: query("SELECT 1"); REQUESTS.labels("/health","200").inc(); return {"status":"ok"}
    except Exception as exc: REQUESTS.labels("/health","503").inc(); raise HTTPException(503,str(exc))

@app.get("/api/v1/grid/live")
def live_grid():
    start=time.perf_counter(); rows=query("SELECT DISTINCT ON(grid_zone) grid_zone,window_start,consumption_kwh,solar_kwh,net_grid_kwh,renewable_pct,active_households FROM zone_metrics ORDER BY grid_zone,window_start DESC")
    REQUESTS.labels("/api/v1/grid/live","200").inc(); LATENCY.labels("/api/v1/grid/live").observe(time.perf_counter()-start); return rows

@app.get("/api/v1/billing/{report_date}")
def billing(report_date: str):
    return query("SELECT * FROM daily_billing WHERE report_date=%s ORDER BY estimated_bill DESC",(report_date,))

@app.get("/metrics")
def metrics(): return Response(generate_latest(),media_type=CONTENT_TYPE_LATEST)


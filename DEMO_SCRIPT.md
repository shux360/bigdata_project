# Smart Grid Pulse — Concise Two-Person Demo Script

Target duration: **6.5–7 minutes**. Replace the names before recording.

## Before recording

1. Confirm the platform is running:

   ```powershell
   docker compose ps
   python scripts/demo_check.py
   ```

2. Open these tabs in advance:

   - `README.md` — architecture image
   - `http://localhost:8000/docs`
   - `http://localhost:8080` — Airflow
   - `http://localhost:9090/targets`
   - `http://localhost:9090/rules`
   - `http://localhost:3000/d/smart-grid-overview/smart-grid-pipeline-overview` — Grafana

3. Keep one recent successful green Airflow DAG run visible. Use a large browser zoom and do not show failed historical runs.

## Recording script

### 0:00–0:25 — Introduction — Member 1

**Show:** README title and architecture image.

**Say:**

> Good morning. We are [Member 1] and [Member 2]. Our project, Smart Grid Pulse, monitors live grid demand and solar contribution by zone, then calculates daily household bills using tariff data. We will demonstrate the full pipeline from ingestion to monitoring.

### 0:25–1:10 — Architecture — Member 1

**Show:** Point left to right across the architecture image.

**Say:**

> This is our Kappa-style platform. The Python meter simulator creates continuous readings, while Airflow manages the scheduled tariff workflow. Kafka is the replayable event backbone. Spark Structured Streaming validates and aggregates meter events. PostgreSQL stores results, and FastAPI serves live grid and billing APIs. Prometheus and Grafana provide monitoring.
>
> We chose Kappa because it keeps one main streaming processing path. Lambda architecture would require separate batch and speed implementations, increasing complexity and consistency risk. One simulated day is five minutes in this demonstration.

### 1:10–1:35 — Running services — Member 1

**Show:** Terminal.

**Run:**

```powershell
docker compose ps
```

**Say:**

> Docker Compose runs Kafka, Spark, PostgreSQL, the producer, API, Airflow, Prometheus, and Grafana. This makes the environment reproducible on one machine.

### 1:35–2:15 — Ingestion and processing — Member 1

**Show:** Terminal.

**Run:**

```powershell
docker compose logs --tail=8 meter-producer
docker compose logs --tail=12 spark
```

**Say:**

> The producer sends readings for twenty households every three seconds. Each event includes a unique ID, household, zone, consumption, solar generation, and timestamp.
>
> Spark validates the data, removes duplicate event IDs, calculates net grid energy as consumption minus solar with a minimum of zero, and creates one-minute metrics for each zone. Checkpoints support recovery after restart.

### 2:15–3:00 — Live API — Member 1

**Show:** FastAPI docs. Execute `GET /api/v1/grid/live`.

**Say:**

> This live endpoint returns the latest result for every grid zone. It shows consumption, solar generation, net grid demand, renewable percentage, and active households.
>
> Solar generation reduces net grid demand, which is the main business insight for grid operators. A later refresh shows a newer time window, proving that this is live pipeline output.

### 3:00–3:50 — Airflow workflow — Member 2

**Show:** Airflow `smart_grid_daily` DAG Graph view and a green run.

**Say:**

> The scheduled workflow is managed by this Airflow DAG. The first task, `generate_and_publish_tariffs`, creates daily tariff records for twenty households and publishes them to Kafka.
>
> The second task, `reconcile_daily_billing`, uses those tariffs and processed energy data to calculate each household's estimated bill. It only runs after tariff generation succeeds. Database upserts make retries safe and avoid duplicate results.

### 3:50–4:35 — Billing result — Member 2

**Show:** FastAPI docs. Execute `GET /api/v1/billing/{report_date}` with the date from the successful Airflow run.

**Say:**

> This endpoint displays the final daily billing records. Each row combines a household's energy usage with its tariff and estimated bill. Solar generation reduces the energy imported from the grid before the bill is calculated. The results are sorted by highest estimated bill.

### 4:35–5:25 — Monitoring — Member 2

**Show:** Prometheus Targets, then Rules.

**Say:**

> Prometheus scrapes API metrics every ten seconds. The target is up, confirming that monitoring is active. We also define alerts for API unavailability and an excessive API error rate.

**Show:** Grafana dashboard.

**Say:**

> Grafana visualises Prometheus data. This dashboard shows API availability, requests, error rate, endpoint activity, and latency, helping an operator assess service health quickly.

### 5:25–6:20 — Quality and production improvements — Both

**Member 1 says:**

> Our quality controls include event IDs, validation, Spark deduplication, non-negative net-grid calculations, database constraints, and Spark checkpoints.

**Member 2 says:**

> Airflow task ordering and idempotent upserts protect daily billing. This is a laptop-scale project; production would use replicated Kafka and Spark, managed secrets, TLS, schema management, and stronger cross-batch idempotency.

### 6:20–6:40 — Closing — Both

**Say:**

> In summary, Smart Grid Pulse ingests continuous and scheduled data, processes and reconciles it, serves live grid and billing results, and monitors the API end to end. Thank you.

## Team contribution statement

- **Member 1:** Kafka, meter simulator, Spark streaming, transformations, validation, checkpoints, and testing.
- **Member 2:** Airflow, tariff and billing workflow, PostgreSQL/API, Prometheus, Grafana, and Docker deployment.
- **Joint work:** Architecture decision, integration, report, testing, and demo preparation.

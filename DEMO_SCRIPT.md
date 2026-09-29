# Smart Grid Pulse — 8-Minute Demo Recording Script

This script is designed for the assessment's required **5–10 minute** video. Aim for 7:30–8:30. Replace `[NAME]` and `[STUDENT ID]` before recording.

## Before recording

1. Start Docker Desktop and wait until its engine is ready.
2. From the repository root, run:

   ```powershell
   docker compose up --build -d
   docker compose ps
   ```

3. Wait until Kafka, PostgreSQL, and ZooKeeper are healthy and the other services show `Up`. First startup can take several minutes, especially Airflow and Spark.
4. Confirm the API and streaming output:

   ```powershell
   python scripts/demo_check.py
   docker compose logs --tail=20 meter-producer spark
   ```

5. Get the Airflow password before recording:

   ```powershell
   docker compose logs airflow | Select-String -Pattern "password"
   ```

6. Open these tabs in advance:

   - Repository `README.md`, with the architecture diagram visible
   - `http://localhost:8000/docs`
   - `http://localhost:8000/api/v1/grid/live`
   - `http://localhost:8080` (Airflow)
   - `http://localhost:9090/targets`
   - `http://localhost:9090/rules`
   - `http://localhost:3000` (Grafana; `admin` / `admin`)
   - The `data/batch` and `data/reports` folders

7. Trigger the Airflow DAG once before recording if you want guaranteed billing output. During the recording, trigger it again to demonstrate retry-safe upserts. Use the date shown by the Airflow run when opening the billing API.
8. Close notifications, enlarge terminal/browser text, and record at 1080p. Do not show passwords or unrelated tabs.

## Recording script

### 0:00–0:35 — Introduction and business question

**Show:** The title at the top of `README.md` and then the architecture diagram.

**Say:**

> Hello, I am [NAME], student ID [STUDENT ID]. This project is Smart Grid Pulse, built for the Smart Grid Energy Monitoring and Billing use case. The utility needs to answer two connected questions: what are the current grid load and solar contribution in each zone, and what will each household's bill be after the daily tariff is applied? This demo shows both the continuously arriving meter stream and the once-per-simulated-day tariff feed running end to end.

### 0:35–1:35 — Architecture decision and technology rationale

**Show:** Trace the README architecture from left to right with the pointer.

**Say:**

> I selected a Kappa-style architecture. Kafka is the replayable event backbone, so real-time events have one primary processing path instead of separate speed and batch implementations with duplicated business logic. This reduces consistency risk and operational cost. A Lambda design could make very large historical recomputation explicit, but it would require two processing paths whose results must be kept equivalent. For this near-real-time, modest-volume scenario, replaying Kafka and using a separate scheduled reference-data workflow is the simpler trade-off.
>
> Python produces smart-meter readings every three seconds for twenty households. Kafka provides durable, keyed ingestion. Spark Structured Streaming validates, enriches, deduplicates within each micro-batch, calculates net grid import, and aggregates one-minute event-time windows with a two-minute watermark. PostgreSQL is appropriate for queryable relational billing and the demonstration volume. FastAPI serves the latest metrics and bills. Airflow schedules the daily tariff and reconciliation workflow. Prometheus evaluates health and error-rate rules, and Grafana connects to those metrics.

**Point out:** One simulated day equals five minutes.

### 1:35–2:20 — Prove the stack is running

**Show:** A terminal in the repository root.

**Run:**

```powershell
docker compose ps
```

**Say:**

> Docker Compose makes the environment reproducible. Here are the running services: ZooKeeper and Kafka for ingestion, Spark for stream processing, PostgreSQL for storage, the meter producer, FastAPI, Airflow, Prometheus, and Grafana. The exposed ports are 8000 for the API, 8080 for Airflow, 9090 for Prometheus, and 3000 for Grafana.

**Run:**

```powershell
python scripts/demo_check.py
```

**Say:**

> This smoke check calls the database-backed health endpoint, the live grid endpoint, and the Prometheus metrics endpoint. HTTP 200 responses show that storage, serving, and metric export are reachable.

### 2:20–3:10 — Streaming ingestion and structured logs

**Show:** The terminal.

**Run:**

```powershell
docker compose logs --tail=12 meter-producer
```

**Say:**

> These JSON log records are evidence of continuous ingestion. Each event has a unique event ID and a household ID. Kafka messages are keyed by household so the same household has stable partition ordering. Measurements include consumption, solar generation, zone, and an event timestamp. The simulator prevents negative source measurements, while Spark also rejects null IDs and negative values as a defensive quality check.

**Run:**

```powershell
docker compose logs --tail=25 spark
```

**Say:**

> Spark consumes the meter-readings topic using Structured Streaming. It computes net imported energy as the maximum of zero and consumption minus solar. It then produces one-minute zone aggregates: total consumption, solar, net grid draw, active households, and renewable percentage. The checkpoint stores progress for restart recovery, and the watermark bounds late event state.

### 3:10–4:05 — Live serving result

**Show:** `http://localhost:8000/docs`. Expand `GET /api/v1/grid/live`, click **Try it out**, then **Execute**. Alternatively refresh the pre-opened JSON endpoint.

**Say:**

> FastAPI exposes a documented serving layer. This endpoint returns the most recent completed or processed one-minute window for each grid zone. For each row, I can see total consumption, solar contribution, net grid import, renewable percentage, and the number of active households. The timestamp changing after refresh demonstrates that the result comes from the running stream rather than a static file.

**Point out:** Two different zone rows and their `renewable_pct` values. Do not read every number.

### 4:05–5:20 — Daily source and Airflow orchestration

**Show:** Airflow at `http://localhost:8080`. Open the `smart_grid_daily` DAG and its graph view.

**Say:**

> The second source is deliberately different: it produces one CSV per simulated day. Airflow runs this DAG every five minutes, matching the compressed clock. The first task generates tariff rows for all twenty households and publishes them to the tariff-updates Kafka topic. The second task upserts the tariff reference data into PostgreSQL, joins it with accumulated meter readings, calculates each estimated bill, and writes a consolidated daily CSV.

**Do:** Trigger the DAG using the play button. Show the two tasks changing to green. If it takes time, briefly show the already-successful previous run, then return to the current run near the end.

**Say:**

> The dependency ensures reconciliation does not run until tariff generation succeeds. Both tariff loading and daily billing use upserts, so an Airflow retry updates the same logical records rather than creating duplicate bills.

### 5:20–6:15 — Consolidated billing result

**Show:** The newly generated file in `data/batch`, then the matching file in `data/reports`. Open the report CSV and display its header plus several rows.

**Say:**

> This is the physical daily tariff input, and this is the final consolidated report required by the business question. Each household row combines streaming consumption and solar totals with the daily rate, tier, and subsidy flag. Net grid energy is never below zero, and the estimated bill is net grid kilowatt-hours multiplied by the tariff rate. Solar therefore reduces imported energy before billing. The database primary key on report date and household supports one reconciled row per household per day.

**Show:** In FastAPI docs, execute `GET /api/v1/billing/{report_date}` using the exact logical date displayed by Airflow.

**Say:**

> The same reconciled data is queryable through the serving API and sorted by highest estimated bill, which lets the utility identify the largest charges quickly.

### 6:15–7:05 — Observability

**Show:** `http://localhost:9090/targets`.

**Say:**

> Observability is part of the pipeline rather than an afterthought. Prometheus scrapes the API every ten seconds. This target is up, proving metric collection is active.

**Show:** `http://localhost:9090/rules` and expand the smart-grid group.

**Say:**

> The first alert becomes critical if the API is unreachable for one minute. The second warns when the two-minute API error ratio exceeds five percent for two minutes. Together with component JSON logs, Airflow task history, Kafka offsets, and Spark checkpoints, these signals support detection and diagnosis. Grafana is provisioned with Prometheus as its data source for visualization.

**Show:** Grafana's Prometheus data source page or Explore view. Do not claim a custom dashboard exists unless you created one.

### 7:05–7:50 — Correctness, trade-offs, and production improvements

**Show:** Return to the architecture diagram or repository map.

**Say:**

> The design includes several safeguards: source and Spark validation, UUID event keys, micro-batch deduplication, database constraints, Airflow task ordering, retry-safe tariff and billing upserts, and Spark checkpoint recovery. However, this is a laptop-scale demonstration, not a production cluster. It uses one Kafka broker, one Spark process, PostgreSQL, simple credentials, and no schema registry. Deduplication is within a Spark micro-batch; replay after deleting checkpoints would need a database upsert or merge to provide stronger end-to-end idempotency.
>
> At production scale I would add replicated Kafka and Spark deployments, partitioned time-series storage, TLS and managed secrets, a schema registry, a dead-letter or quarantine stream, stronger cross-batch idempotency, and alerting for missing meter data and unusually low renewable contribution.

### 7:50–8:05 — Closing

**Show:** The live endpoint one last time and refresh it.

**Say:**

> In summary, the system ingests continuous and daily data, performs meaningful event-time transformation and reconciliation, serves live and daily answers, and exposes operational health. That completes the end-to-end Smart Grid Pulse demonstration.

## If something is slow during recording

- If Spark is still downloading packages, pause before recording; do not record startup waiting time.
- If the latest grid endpoint is initially empty, wait for one Spark trigger interval and refresh.
- If an Airflow run takes too long, show a previous green run and its task logs, then show its generated report file.
- If the billing endpoint is empty, verify that the date parameter exactly matches the Airflow run's logical date and that meter readings exist for that date.
- If solar is zero, explain that the simulator produces solar only from 06:00 through 18:00 UTC; do not describe it as a failure.
- Do not stop the API merely to force an alert unless you have rehearsed recovery. Showing the loaded rule and healthy target satisfies the observability demonstration more safely.

## Final recording checklist

- Keep the video between 5 and 10 minutes.
- Show both simulated sources, not only the API.
- Show actual changing live output and the consolidated billing output.
- Explicitly say why Kappa was chosen and why Lambda was rejected.
- Tie every technology to this use case.
- Show structured logs, Prometheus target health, and alert rules.
- State the five-minute simulated day and the limitations honestly.
- If this is a group submission, add a final contribution slide or a brief spoken contribution statement.

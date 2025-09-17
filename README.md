
# Modular ELT Pipeline with Supabase & Power BI

## Dashboard Preview
![Recruitment Dashboard Preview](source/image.png)

## Overview

This project implements a modular **ELT (Extract → Load → Transform)** pipeline for analytical workloads.
Data is ingested from Excel sources, loaded into **Supabase (Postgres)**, staged, cleaned, and transformed with **dbt**, then served for analytics.

Final data is exposed via:

* **Supabase REST API endpoints** (per table/view).
* **Direct PostgreSQL connections** for BI tools.
* **Power BI dashboards** consuming curated tables.

The pipeline is **containerized with Docker** and **orchestrated via Apache Airflow**, running both manually (for demo) and on a scheduler (for production-like automation).

---

## Architecture

```text
          +-----------------+
          | Excel Sources   |
          +-----------------+
                   |
                   v
          +-----------------+
          |   Extraction    |  (Python in Airflow DAG)
          +-----------------+
                   |
                   v
          +-----------------+
          |    Raw Layer    |  (Supabase/Postgres)
          +-----------------+
                   |
                   v
          +-----------------+
          |   Staging Layer |  (dbt transformations)
          +-----------------+
                   |
                   v
          +-----------------+
          | Business Logic  |  (dbt models → analytics schema)
          +-----------------+
                   |
                   v
          +-----------------+
          |    Serving      |  (Supabase REST / Views)
          +-----------------+
                   |
          +---------------------+
          | Power BI Dashboards |
          +---------------------+
```

---

## Tech Stack

* **Python** – ingestion scripts (Excel → Postgres)
* **PostgreSQL (Supabase)** – warehouse
* **dbt** – transformations (staging + business logic)
* **Docker** – containerized environment
* **Apache Airflow (CLI only)** – pipeline orchestration & scheduling
* **Supabase REST API** – serving layer
* **Power BI** – visualization

---

## Project Structure

```bash
.
├── dags/                # Airflow DAG definitions (elt_dbt_pipeline)
├── script/              # Python scripts for Excel ingestion
├── dbt/                 # dbt models (staging + analytics)
├── docker-compose.yml   # Docker services (Airflow, dbt, Postgres)
├── requirements.txt     # Python dependencies
└── README.md
```

---

## Setup & Deployment

### 1. Clone Repo

```bash
git clone https://github.com/your-username/elt-pipeline.git
cd elt-pipeline
```

### 2. Environment Variables

Create `.env` file:

```bash
POSTGRES_HOST=...
POSTGRES_DB=...
POSTGRES_USER=...
POSTGRES_PASSWORD=...
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
```

### 3. Start Containers

Build and start services:

```bash
docker-compose up --build

docker exec -it airflow bash #(moving inside container to run the dags manually for testing)
```

### 4. Run Pipeline (Demo Mode)

List DAGs:

```bash
airflow dags list
```

Trigger the pipeline:

```bash
airflow dags trigger elt_dbt_pipeline
```

This will:

1. Extract data from Excel and load into **raw schema** in Supabase.
2. Run **dbt staging models** to clean/standardize data.
3. Run **dbt business logic models** to build analytics tables.
4. Push final curated tables to Supabase for serving.

---

### 5. Scheduler (Production Mode)

Airflow is configured with a scheduler that automatically runs `elt_dbt_pipeline` at predefined intervals.
This ensures continuous ingestion, transformation, and serving without manual triggers.

---

### 6. Data Access Layer

## Analytics Tables (in `analytics` schema)

| Table                                | Description                                                |
| ------------------------------------ | ---------------------------------------------------------- |
| **unified_intake**                  | Unified intake data across all cohorts.                    |
| **fact_performance_metrics**       | Cohort-level performance metrics.                          |
| **fact_application_details**       | Detailed application data per cohort.                      |
| **dim_applicant_demographics**     | Demographic dimension of applicants.                       |
| **mart_total_intake_summary**     | Aggregate summary of total intakes and trends.             |
| **mart_recruitment_effectiveness** | Metrics evaluating recruitment channels and effectiveness. |

---

##  Access Layer (REST API)

* **Project URL:** `https://hsiovdjiuffpwimlhhok.supabase.co`
* **Anon Key:**

  ```
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhzaW92ZGppdWZmcHdpbWxoaG9rIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTc1MTU0MjUsImV4cCI6MjA3MzA5MTQyNX0.oEUyQZSHG4kVJ9AQT-gfxnPKNGzOCqVWsL_tt2bo_2g
  ```

### Headers required for all endpoints

```bash
-H "apikey: YOUR_ANON_KEY" \
-H "Authorization: Bearer YOUR_ANON_KEY"
```

### Base Request Format

**JSON response (default):**

```bash
curl "https://hsiovdjiuffpwimlhhok.supabase.co/rest/v1/{table_name}" \
  -H "apikey: YOUR_ANON_KEY" \
  -H "Authorization: Bearer YOUR_ANON_KEY"
```

**CSV download:**

```bash
curl "https://hsiovdjiuffpwimlhhok.supabase.co/rest/v1/{table_name}" \
  -H "apikey: YOUR_ANON_KEY" \
  -H "Authorization: Bearer YOUR_ANON_KEY" \
  -H "Accept: text/csv" \
  -o {table_name}.csv
```

Replace `{table_name}` with one of the names in the Analytics Tables section above.

---

### 7. Power BI

I used the rest endpoints but you could connect directly though requires ssl:

* Connect to Supabase **REST endpoints** (each table separately).

---

## Contribution

Contributions are welcome!

1. Fork the repo
2. Create a feature branch
3. Commit changes
4. Open a PR

---

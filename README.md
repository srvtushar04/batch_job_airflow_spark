# batch_job_airflow_spark

# End-to-End Batch Data Pipeline (Airflow + PySpark + Snowflake)

A production-style batch ETL showcasing:
- **Airflow** for orchestration
- **PySpark** (local mode) for scalable transforms
- **Data quality checks** (simple, fast, extensible)
- **Snowflake** loading (optional via env vars; else saved locally)
- **Docker Compose** for one-command local run

## 🔧 What the pipeline does
1) **Extract**: Fetches a sample product catalog from a public API (falls back to synthetic data if offline). Saves raw JSONL to `./data/raw/`.
2) **Transform**: Cleans and normalizes with **PySpark** (dedupe, schema typing, basic enrichments). Writes Parquet to `./data/processed/`.
3) **Quality**: Runs quick checks (row count, null thresholds, unique keys). Fails the DAG if checks fail.
4) **Load**: If Snowflake creds provided, upserts to a target table. If not, writes a final Parquet+CSV to `./data/final/`.

## 🧱 Stack
- Python 3.11
- Apache Airflow 2.9
- PySpark 3.5
- pandas, pyarrow
- snowflake-connector-python (optional)

## ▶️ Quickstart
```bash
# 1) Copy this folder and cd into it
cd project1_batch_airflow_spark

# 2) Prepare env file
cp .env.example .env
# (Optional) edit .env to add Snowflake credentials

# 3) Start the stack
docker-compose up -d --build

# 4) Access Airflow UI (username: airflow / password: airflow)
# http://localhost:8080

# 5) In Airflow UI, unpause and trigger DAG: etl_pipeline

# batch_job_airflow_spark

# End-to-End Batch ETL (Airflow + PySpark + GE + Snowflake)

**Flow:** API → S3 (MinIO) Raw → PySpark Transform → GE Validate → Snowflake

## Run
1) `cp .env.example .env` and fill Snowflake creds (or skip Load step if you don't have Snowflake).
2) `docker-compose up -d`
3) Open Airflow: http://localhost:8080 (admin/admin)
4) Trigger DAG `etl_pipeline`.

MinIO console: http://localhost:9001 (minioadmin/minioadmin).  
Buckets: `raw-zone`, `processed-zone`.

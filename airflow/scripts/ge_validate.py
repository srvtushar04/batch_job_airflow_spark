import os
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import great_expectations as ge
from great_expectations.core.batch import BatchRequest

def run(**context):
    ds = context.get("ds") or datetime.utcnow().strftime("%Y-%m-%d")
    bucket = os.environ.get("PROCESSED_BUCKET", "processed-zone")
    prefix = os.environ.get("PROCESSED_PREFIX", f"products_clean/dt={ds}")
    endpoint = os.environ.get("SPARK_S3_ENDPOINT")

    # Download parquet to local temp via MinIO presigned-less (direct since it's local & open)
    # Simpler: use s3fs via pandas read_parquet(engine="pyarrow", storage_options=...)
    path = f"s3://{bucket}/{prefix}"
    df = pd.read_parquet(
        path,
        engine="pyarrow",
        storage_options={
            "client_kwargs": {"endpoint_url": endpoint},
            "key": os.environ.get("SPARK_S3_ACCESS_KEY"),
            "secret": os.environ.get("SPARK_S3_SECRET_KEY"),
        },
    )

    ge_df = ge.from_pandas(df)

    # Core checks
    ge_df.expect_column_values_to_not_be_null("id")
    ge_df.expect_column_values_to_be_unique("id")
    ge_df.expect_table_row_count_to_be_between(min_value=1, max_value=1000000)
    ge_df.expect_column_values_to_be_between("price", min_value=0, max_value=1_000_000)
    ge_df.expect_column_values_to_match_regex("category", r"^[A-Za-z0-9_\- ]+$")

    result = ge_df.validate()
    print(result)

    if not result["success"]:
        raise AssertionError("Great Expectations validation failed")

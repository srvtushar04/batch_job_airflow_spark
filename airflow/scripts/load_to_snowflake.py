import os
import pandas as pd

def _have_sf_env():
    req = ["SNOWFLAKE_USER","SNOWFLAKE_PASSWORD","SNOWFLAKE_ACCOUNT",
           "SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA"]
    return all(os.environ.get(k) for k in req)

def run(**context):
    if not _have_sf_env():
        print("Snowflake env not set; skipping load step.")
        return

    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    endpoint = os.environ.get("SPARK_S3_ENDPOINT")
    bucket = os.environ.get("PROCESSED_BUCKET", "processed-zone")
    prefix = os.environ.get("PROCESSED_PREFIX", f"products_clean/dt={context.get('ds')}")
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

    table = "PRODUCTS_CLEAN"
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                create table if not exists {table} (
                    id number,
                    title string,
                    category string,
                    brand string,
                    price float,
                    rating float,
                    stock number,
                    tags variant,
                    warrantyInformation string,
                    shippingInformation string,
                    weight float,
                    width float,
                    height float,
                    depth float,
                    created_at date,
                    source string,
                    dt string,
                    is_price_reasonable boolean
                );
            """)
        # Load via write_pandas
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name=table, quote_identifiers=False)
        print(f"Snowflake write_pandas success={success}, chunks={nchunks}, rows={nrows}")
    finally:
        conn.close()

import json
import os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T

def _spark():
    cfg_path = "/opt/airflow/configs/spark_config.json"
    with open(cfg_path) as f:
        cfg = json.load(f)

    builder = (
        SparkSession.builder
        .appName("transform_products")
    )
    for k, v in cfg.items():
        builder = builder.config(k, v)

    # S3 creds
    endpoint = os.environ.get("SPARK_S3_ENDPOINT")
    ak = os.environ.get("SPARK_S3_ACCESS_KEY")
    sk = os.environ.get("SPARK_S3_SECRET_KEY")

    builder = (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    return builder.getOrCreate()

def run(**context):
    ds = context.get("ds") or datetime.utcnow().strftime("%Y-%m-%d")
    raw_bucket = os.environ.get("RAW_BUCKET", "raw-zone")
    raw_prefix = os.environ.get("RAW_PREFIX", f"products_api/dt={ds}")
    processed_bucket = os.environ.get("PROCESSED_BUCKET", "processed-zone")
    processed_prefix = os.environ.get("PROCESSED_PREFIX", f"products_clean/dt={ds}")

    spark = _spark()

    src = f"s3a://{raw_bucket}/{raw_prefix}/*.json.gz"
    df = spark.read.json(src, multiLine=False)

    # Flatten dimensions if present
    for c in ["width", "height", "depth"]:
        df = df.withColumn(c, F.col(f"dimensions.{c}").cast("double"))
    df = df.drop("dimensions")

    # Type casts, defaults
    df = (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("stock", F.col("stock").cast("long"))
        .withColumn("weight", F.col("weight").cast("double"))
        .withColumn("created_at", F.to_date("created_at"))
        .withColumn("brand", F.coalesce(F.col("brand"), F.lit("Unknown")))
        .withColumn("category", F.coalesce(F.col("category"), F.lit("misc")))
        .dropDuplicates(["id"])
        .filter("price >= 0 and stock >= 0")
        .withColumn("dt", F.lit(ds))
    )

    # Basic quality indicators for later GE checks
    df = df.withColumn("is_price_reasonable", (F.col("price") <= 1e6).cast("boolean"))

    dest = f"s3a://{processed_bucket}/{processed_prefix}"
    (
        df.repartition(1)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(dest)
    )
    print(f"Wrote {dest}")
    spark.stop()

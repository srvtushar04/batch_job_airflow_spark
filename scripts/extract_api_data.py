import gzip
import io
import json
import os
from datetime import datetime
import requests
import boto3
from botocore.config import Config

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("SPARK_S3_ENDPOINT"),
        aws_access_key_id=os.environ.get("SPARK_S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("SPARK_S3_SECRET_KEY"),
        config=Config(s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )

def _fetch_products():
    # Pull multiple pages to simulate volume
    all_items = []
    limit = 100
    total = 200  # 2 pages
    for skip in range(0, total, limit):
        url = f"https://dummyjson.com/products?limit={limit}&skip={skip}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        all_items.extend(resp.json().get("products", []))
    return all_items

def run(execution_date: str = None, **context):
    ds = context.get("ds") or datetime.utcnow().strftime("%Y-%m-%d")
    bucket = os.environ.get("RAW_BUCKET", "raw-zone")
    prefix = os.environ.get("RAW_PREFIX", f"products_api/dt={ds}")

    items = _fetch_products()

    # Normalize a bit & add metadata
    enriched = []
    for p in items:
        enriched.append({
            "id": p.get("id"),
            "title": p.get("title"),
            "category": p.get("category"),
            "brand": p.get("brand"),
            "price": p.get("price"),
            "rating": p.get("rating"),
            "stock": p.get("stock"),
            "tags": p.get("tags"),
            "warrantyInformation": p.get("warrantyInformation"),
            "shippingInformation": p.get("shippingInformation"),
            "weight": p.get("weight"),
            "dimensions": p.get("dimensions"),
            "created_at": ds,
            "source": "dummyjson",
        })

    # Write a gzipped JSONL object
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for row in enriched:
            gz.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))

    key = f"{prefix}/part-0000.json.gz"
    s3 = _s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="application/json")
    print(f"Wrote s3://{bucket}/{key} ({len(enriched)} records)")

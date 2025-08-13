import json
import os
from datetime import datetime
from typing import Optional

import requests

API_URL = "https://dummyjson.com/products?limit=100"


def _fetch_from_api() -> Optional[list]:
    try:
        r = requests.get(API_URL, timeout=20)
        r.raise_for_status()
        data = r.json()
        products = data.get("products", [])
        return products
    except Exception:
        return None


def _generate_synthetic(n: int = 200) -> list:
    from random import randint, random, choice
    cats = ["laptop", "phone", "audio", "home", "beauty"]
    res = []
    for i in range(1, n + 1):
        res.append(
            {
                "id": i,
                "title": f"Product {i}",
                "description": f"Synthetic product {i}",
                "category": choice(cats),
                "price": round(randint(1000, 90000) * (1 + random() * 0.1), 2),
                "rating": round(3 + random() * 2, 2),
                "brand": f"Brand{(i % 10) + 1}",
                "stock": randint(0, 500),
            }
        )
    return res


def run(run_date: str, output_dir: str) -> str:
    """Writes JSONL to output_dir/products_YYYYMMDD.jsonl and returns t

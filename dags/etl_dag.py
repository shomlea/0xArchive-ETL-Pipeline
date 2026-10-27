import os
import sqlite3
import requests
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv

from airflow.decorators import dag, task

load_dotenv()
API_KEY = os.getenv("OXARCHIVE_API_KEY")
BASE_URL = "https://api.0xarchive.io/v1"
DB_PATH = "/opt/airflow/data/market_data.db"

default_args = {
    "owner": "data_engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5)
}

@dag(
    dag_id="0xarchive_crypto_etl",
    default_args=default_args,
    description="Hourly ETL pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["crypto", "etl", "0xarchive"]
)
def etl_pipeline():
    @task()
    def extract(venue: str, symbol: str, interval: str, limit: int):
        url = f"{BASE_URL}/{venue}/candles/{symbol}"
        headers = {
            "X-API-Key": API_KEY,
            "Accept": "application/json"
        }
        params = {
            "interval": interval,
            "limit": limit
        }

        response=requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"0xArchive API Error ({response.status_code}): {response.text}")

        return {
            "raw": response.json(),
            "venue": venue,
            "symbol": symbol
        }

    @task()
    def transform(extracted_payload: dict):
        records = extracted_payload["raw"].get("data", [])
        venue = extracted_payload["venue"]
        symbol = extracted_payload["symbol"]

        if not records:
            return []

        cleaned_rows = []
        for row in records:
            cleaned_rows.append({
                "venue": venue,
                "symbol": symbol,
                "timestamp": pd.to_datetime(row.get("timestamp"), unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S"),
                "open": float(row.get("open")),
                "high": float(row.get("high")),
                "low": float(row.get("low")),
                "close": float(row.get("close")),
                "volume": float(row.get("volume", 0)),
            })

        df = pd.DataFrame(cleaned_rows)
        df["price_spread"] = (df["high"] - df["low"]).round(2)
        df["return_pct"] = (((df["close"] - df["open"]) / df["open"]) * 100).round(3)

        return df.to_dict(orient="records")

    @task()
    def load(cleaned_records: list):
        if not cleaned_records:
            print("No records to insert.")
            return

        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
                    CREATE TABLE IF NOT EXISTS crypto_candles (
                        venue TEXT,
                        symbol TEXT,
                        timestamp TEXT,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        price_spread REAL,
                        return_pct REAL,
                        PRIMARY KEY (venue, symbol, timestamp)
                    )
                """)

        inserted = 0
        for row in cleaned_records:
            cursor.execute("""
                        INSERT OR IGNORE INTO crypto_candles
                        (venue, symbol, timestamp, open, high, low, close, volume, price_spread, return_pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                row["venue"], row["symbol"], row["timestamp"],
                row["open"], row["high"], row["low"], row["close"],
                row["volume"], row["price_spread"], row["return_pct"]
            ))
            inserted += cursor.rowcount

        conn.commit()
        conn.close()

    raw_data = extract(venue = "hyperliquid", symbol = "BTC", interval = "1h", limit = 50)
    transformed_data = transform(raw_data)
    load(transformed_data)


etl_pipeline()
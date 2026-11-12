# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ba9cfdb7-cf33-423d-8a4c-21d224e17ea8",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "c70900d4-8446-43f4-8a7d-4bca5f785f14",
# META       "known_lakehouses": [
# META         {
# META           "id": "ba9cfdb7-cf33-423d-8a4c-21d224e17ea8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pathlib import Path
import requests

API_KEY = Path("/lakehouse/default/Files/config/openaq.key").read_text().strip()
BASE_URL = "https://api.openaq.org/v3"
SENSORS = [673, 1097, 1102, 1103, 1152, 1128, 1145, 1146, 25520, 2780575, 1535, 23341]
DATE_FROM = "2024-01-01"
DATE_TO = "2026-11-01"
PAGE_SIZE = 1000

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_page(sensor_id, page):
    response = requests.get(
        f"{BASE_URL}/sensors/{sensor_id}/days",
        headers={"X-API-Key": API_KEY},
        params={"date_from": DATE_FROM, "date_to": DATE_TO, "limit": PAGE_SIZE, "page": page},
    )
    response.raise_for_status()
    return response.json()["results"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_sensor(sensor_id):
    rows = []
    page = 1
    while True:
        results = get_page(sensor_id, page)
        for row in results:
            row["sensor_id"] = sensor_id
        rows.extend(results)
        if len(results) < PAGE_SIZE:
            return rows
        page += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

all_rows = [row for sensor_id in SENSORS for row in get_sensor(sensor_id)]
pdf = pd.json_normalize(all_rows)
pdf.columns = [c.replace(".", "_") for c in pdf.columns]

print(pdf.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdf = pdf.dropna(axis=1, how="all")

spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable("openaq_days")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = [f"{year}-{month:02d}" for year in (2024, 2025, 2026) for month in range(1, 13)]

taxi_dir = Path("/lakehouse/default/Files/taxi")
taxi_dir.mkdir(parents=True, exist_ok=True)


def download_month(month):
    name = f"yellow_tripdata_{month}.parquet"
    path = taxi_dir / name
    if path.exists():
        return f"{month}: skipped, already downloaded"
    response = requests.get(f"{TAXI_URL}/{name}")
    if response.status_code != 200:
        return f"{month}: not published ({response.status_code})"
    path.write_bytes(response.content)
    return f"{month}: {len(response.content) // 1024 // 1024} MB"


for month in MONTHS:
    print(download_month(month))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = sorted(taxi_dir.glob("*.parquet"))
print(len(files), "files,", sum(f.stat().st_size for f in files) // 1024 // 1024, "MB")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

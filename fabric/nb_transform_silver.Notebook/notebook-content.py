# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0a6090cb-ca0c-4f49-899a-c0684c6a5252",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "c70900d4-8446-43f4-8a7d-4bca5f785f14",
# META       "known_lakehouses": [
# META         {
# META           "id": "0a6090cb-ca0c-4f49-899a-c0684c6a5252"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # Silver layer
# 
# Reads lh_bronze, writes clean typed tables to lh_silver.
# Bronze is not modified. Everything here can be rebuilt by rerunning.
# 
# All tables carry source_system and loaded_at.

# CELL ********************

from pyspark.sql import functions as F

WORKSPACE = "fabric-urban-analytics"
ONELAKE = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com"
BRONZE = f"{ONELAKE}/lh_bronze.Lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FX rates (ECB)
# 
# Text to date and decimal(18,6).
# Decimal, not double: these rates convert revenue in Gold, double loses precision on money.

# CELL ********************

fx_bronze = spark.read.format("delta").load(f"{BRONZE}/Tables/ecb_fx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx_silver = (
    fx_bronze
    .select(
        F.to_date("TIME_PERIOD", "yyyy-MM-dd").alias("rate_date"),
        F.col("OBS_VALUE").cast("decimal(18,6)").alias("usd_per_eur"),
    )
    .filter(F.col("rate_date").isNotNull() & F.col("usd_per_eur").isNotNull())
    .dropDuplicates(["rate_date"])
    .withColumn("source_system", F.lit("ECB"))
    .withColumn("loaded_at", F.current_timestamp())
)

print(fx_silver.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_fx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GDP (World Bank)
# 
# date is a year, keep as int. 2025-01-01 would mean one day.
# decimal(20,2): US GDP is 14 digits, a smaller decimal overflows to null.

# CELL ********************

gdp_bronze = spark.read.format("delta").load(f"{BRONZE}/Tables/worldbank_gdp")

gdp_silver = (
    gdp_bronze
    .select(
        F.col("countryiso3code").alias("country_code"),
        F.col("date").cast("int").alias("year"),
        F.col("value").cast("decimal(20,2)").alias("gdp_usd"),
    )
    .filter(F.col("year").isNotNull() & F.col("gdp_usd").isNotNull())
    .dropDuplicates(["country_code", "year"])
    .withColumn("source_system", F.lit("World Bank"))
    .withColumn("loaded_at", F.current_timestamp())
)

print(gdp_silver.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gdp_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_gdp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Air quality (OpenAQ)
# 
# 11265 rows, 12 sensors, 2 parameters.
# 
# What I found when profiling:
# - pm25 in µg/m³ (9210 rows), no2 in ppm (2055 rows). Units differ, so these
#   parameters can never be aggregated together
# - the date is inside period_datetimeFrom_local, format 2024-01-01T00:00:00-05:00.
#   I use local time because a daily value belongs to the local calendar day
# - coverage_percentComplete goes from 4 to 104 percent. Above 100 happens on the
#   day the clock changes, when the sensor reports 25 hours against 24 expected
# 
# Cleaning rules:
# 1. keep days with coverage >= 75 percent. This is the EPA threshold for a valid
#    daily average. It drops 260 rows of 11265
# 2. drop negative concentrations. 7 rows, sensor noise near the detection limit
# 3. deduplicate on sensor, date and parameter
# 
# I also checked the 3 lowest no2 values. Different sensors, different dates,
# 100 percent coverage. These are real clean days, so I kept them.

# CELL ********************

air_bronze = spark.read.format("delta").load(f"{BRONZE}/Tables/openaq_days")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

air_silver = (
    air_bronze
    .filter(F.col("coverage_percentComplete") >= 75)
    .select(
        F.col("sensor_id"),
        F.to_date(F.substring("period_datetimeFrom_local", 1, 10)).alias("measure_date"),
        F.col("parameter_name").alias("parameter"),
        F.col("parameter_units").alias("unit"),
        F.col("value").alias("value_avg"),
        F.col("summary_max").alias("value_max"),
        F.col("coverage_percentComplete").alias("coverage_pct"),
    )
    .filter(F.col("measure_date").isNotNull() & F.col("value_avg").isNotNull() & (F.col("value_avg") >= 0))
    .dropDuplicates(["sensor_id", "measure_date", "parameter"])
    .withColumn("source_system", F.lit("OpenAQ"))
    .withColumn("loaded_at", F.current_timestamp())
)

print(air_silver.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

air_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_air_quality")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Taxi trips (NYC TLC)
# 
# 33 files, 2004 MB. Profiling 2024-01: pickup dates from 2002, distance up to 312722 miles,
# total down to -900, trips with 0 passengers.
# 
# Rules are in clean_taxi. Two need a note:
# 
# Bad passenger_count nulls the field, not the row. The trip is real.
# Dedup per month, not global: no key, and a full shuffle over 117M rows does not fit F2.
# Duplicates between months are impossible, each month is a separate file.
# 
# Result: 117618292 trips, 2024-01-01 to 2026-09-30.

# CELL ********************

TAXI_DIR = f"{BRONZE}/Files/taxi"
WINDOW_START = "2024-01-01"
WINDOW_END = "2026-10-01"
MAX_DISTANCE_MILES = 100
MAX_TOTAL_AMOUNT = 1000
MAX_DURATION_SECONDS = 24 * 60 * 60


def clean_taxi(df):
    trips = (
        df.select(
            F.col("tpep_pickup_datetime").alias("pickup_at"),
            F.col("tpep_dropoff_datetime").alias("dropoff_at"),
            F.col("PULocationID").alias("pickup_zone_id"),
            F.col("DOLocationID").alias("dropoff_zone_id"),
            F.col("passenger_count"),
            F.col("trip_distance"),
            F.col("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
            F.col("tip_amount").cast("decimal(10,2)").alias("tip_amount"),
            F.col("total_amount").cast("decimal(10,2)").alias("total_amount"),
        )
        .withColumn("pickup_date", F.to_date("pickup_at"))
        .withColumn(
            "duration_seconds",
            F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at"),
        )
    )

    return (
        trips.filter(
            (F.col("pickup_date") >= F.lit(WINDOW_START).cast("date"))
            & (F.col("pickup_date") < F.lit(WINDOW_END).cast("date"))
            & (F.col("duration_seconds") > 0)
            & (F.col("duration_seconds") < MAX_DURATION_SECONDS)
            & (F.col("trip_distance") > 0)
            & (F.col("trip_distance") <= MAX_DISTANCE_MILES)
            & (F.col("total_amount") > 0)
            & (F.col("total_amount") <= MAX_TOTAL_AMOUNT)
            & F.col("pickup_zone_id").between(1, 265)
            & F.col("dropoff_zone_id").between(1, 265)
        )
        .withColumn(
            "passenger_count",
            F.when(F.col("passenger_count").between(1, 8), F.col("passenger_count")),
        )
        .withColumn("pickup_month", F.date_format("pickup_at", "yyyy-MM"))
        .dropDuplicates()
        .withColumn("source_system", F.lit("NYC TLC"))
        .withColumn("loaded_at", F.current_timestamp())
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

files = sorted(notebookutils.fs.ls(TAXI_DIR), key=lambda f: f.name)

for index, file in enumerate(files, start=1):
    start = time.time()
    month_clean = clean_taxi(spark.read.parquet(file.path))
    mode = "overwrite" if index == 1 else "append"
    (
        month_clean.write
        .mode(mode)
        .option("overwriteSchema", "true")
        .partitionBy("pickup_month")
        .saveAsTable("silver_taxi")
    )
    print(f"{file.name}: {time.time() - start:.0f}s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_silver = spark.read.table("silver_taxi")

print(f"{taxi_silver.count():,} rows")

taxi_silver.selectExpr(
    "min(pickup_date) as first_day",
    "max(pickup_date) as last_day",
    "count(distinct pickup_month) as months",
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Reference data
# 
# Zones and sensors are already clean at the Bronze stage, but they still pass
# through Silver so that Gold reads from one layer only.
# 
# Zones 264 and 265 are "Unknown" and "N/A" in the TLC lookup. They are kept:
# dropping them would silently lose trips instead of showing them as unknown.
# 
# Sensor to borough mapping is manual, see nb_ingest_openaq. OpenAQ gives
# coordinates but not boroughs.

# CELL ********************

zones_bronze = spark.read.format("delta").load(f"{BRONZE}/Tables/taxi_zones")

zones_silver = (
    zones_bronze
    .select(
        F.col("LocationID").alias("zone_id"),
        F.trim(F.col("Borough")).alias("borough"),
        F.trim(F.col("Zone")).alias("zone_name"),
        F.trim(F.col("service_zone")).alias("service_zone"),
    )
    .filter(F.col("zone_id").isNotNull())
    .dropDuplicates(["zone_id"])
    .withColumn("source_system", F.lit("NYC TLC"))
    .withColumn("loaded_at", F.current_timestamp())
)

print(zones_silver.count())
zones_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_zones")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_bronze = spark.read.format("delta").load(f"{BRONZE}/Tables/openaq_sensors")

sensors_silver = (
    sensors_bronze
    .select("sensor_id", "parameter", "station_name", "borough", "latitude", "longitude")
    .dropDuplicates(["sensor_id"])
    .withColumn("source_system", F.lit("OpenAQ"))
    .withColumn("loaded_at", F.current_timestamp())
)

print(sensors_silver.count())
sensors_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_sensors")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

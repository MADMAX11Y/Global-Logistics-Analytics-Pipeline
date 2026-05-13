"""
Member 6 — Prefect Orchestration
Manages the full ETL pipeline execution end-to-end.

Run: python 06_orchestration.py
Monitor: prefect server start  (then open http://localhost:4200)
"""

from prefect import flow, task, get_run_logger
from datetime import datetime, timedelta
import subprocess
import requests
import pandas as pd
import numpy as np
import duckdb
import os
import sys

# ── CONFIG ────────────────────────────────────────────────────────
FX_API_KEY  = "1c8e87a46f394ce88e8525a67af94523"
TAXI_PATH   = r"C:\Users\hp\Desktop\Pipeline Flow\data\raw\yellow_tripdata_2024-01.parquet"
DB_PATH     = "data/pipeline_analytics.duckdb"

# ══════════════════════════════════════════════════════════════════
#  TASK 1 — EXTRACT
# ══════════════════════════════════════════════════════════════════

@task(name="Extract Countries API", retries=3, retry_delay_seconds=10)
def extract_countries():
    logger = get_run_logger()
    logger.info("🌍 Extracting REST Countries API...")

    url = "https://restcountries.com/v3.1/all?fields=name,population,area,region,subregion,currencies,capital"
    response = requests.get(url, timeout=30)

    if response.status_code != 200:
        raise Exception(f"Countries API failed: {response.status_code}")

    countries_raw = response.json()
    records = []
    for c in countries_raw:
        currencies = c.get("currencies", {})
        currency_code = list(currencies.keys())[0] if currencies else "N/A"
        currency_name = currencies[currency_code]["name"] if currencies and currency_code != "N/A" else "N/A"
        records.append({
            "country_name":  c["name"]["common"],
            "official_name": c["name"]["official"],
            "capital":       c["capital"][0] if c.get("capital") else "N/A",
            "region":        c.get("region", "N/A"),
            "subregion":     c.get("subregion", "N/A"),
            "population":    c.get("population", 0),
            "area_km2":      c.get("area", 0.0),
            "currency_code": currency_code,
            "currency_name": currency_name,
            "extracted_at":  datetime.now().isoformat()
        })

    df = pd.DataFrame(records)
    df = df[df["population"] > 0]

    os.makedirs("data/raw", exist_ok=True)
    df.to_parquet("data/raw/countries_raw.parquet", index=False)
    logger.info(f"✅ Countries extracted: {len(df)} rows saved")
    return len(df)


@task(name="Extract Exchange Rates API", retries=3, retry_delay_seconds=10)
def extract_fx_rates():
    logger = get_run_logger()
    logger.info("💱 Extracting Exchange Rates API...")

    url = f"https://openexchangerates.org/api/latest.json?app_id={FX_API_KEY}"
    response = requests.get(url, timeout=30)

    if response.status_code != 200:
        raise Exception(f"FX API failed: {response.status_code}")

    fx_data = response.json()
    records = [
        {
            "currency_code": code,
            "rate_to_usd":   rate,
            "usd_to_local":  rate,
            "base_currency": fx_data["base"],
            "rate_date":     datetime.fromtimestamp(fx_data["timestamp"]).date().isoformat(),
            "extracted_at":  datetime.now().isoformat()
        }
        for code, rate in fx_data["rates"].items()
    ]

    df = pd.DataFrame(records)
    df.to_parquet("data/raw/fx_rates_raw.parquet", index=False)
    logger.info(f"✅ FX rates extracted: {len(df)} currencies saved")
    return len(df)


@task(name="Extract Taxi Parquet", retries=1)
def extract_taxi():
    logger = get_run_logger()
    logger.info(f"🚕 Loading NYC Taxi Parquet from {TAXI_PATH}...")

    if not os.path.exists(TAXI_PATH):
        raise FileNotFoundError(f"Taxi file not found: {TAXI_PATH}")

    KEEP_COLS = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "total_amount", "payment_type",
        "PULocationID", "DOLocationID"
    ]
    df_full = pd.read_parquet(TAXI_PATH)
    available = [c for c in KEEP_COLS if c in df_full.columns]
    df = df_full[available].copy()

    df.to_parquet("data/raw/taxi_raw.parquet", index=False)
    logger.info(f"✅ Taxi data extracted: {len(df):,} rows saved")
    return len(df)


# ══════════════════════════════════════════════════════════════════
#  TASK 2 — TRANSFORM
# ══════════════════════════════════════════════════════════════════

@task(name="Transform with PySpark", retries=1)
def transform_with_spark():
    logger = get_run_logger()
    logger.info("⚡ Starting PySpark transformations...")

    os.makedirs("data/processed", exist_ok=True)

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.builder \
            .appName("ETLPipeline") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        df_countries = spark.read.parquet("data/raw/countries_raw.parquet")
        df_taxi      = spark.read.parquet("data/raw/taxi_raw.parquet")
        df_fx        = spark.read.parquet("data/raw/fx_rates_raw.parquet")

        df_countries_t = df_countries \
            .filter(F.col("area_km2") > 0) \
            .withColumn("population_density", F.round(F.col("population") / F.col("area_km2"), 2)) \
            .withColumn("market_size_category",
                F.when(F.col("population") > 100_000_000, "MEGA")
                 .when(F.col("population") > 10_000_000, "LARGE")
                 .when(F.col("population") > 1_000_000, "MEDIUM")
                 .otherwise("SMALL")) \
            .withColumn("density_category",
                F.when(F.col("population_density") > 500, "VERY_HIGH")
                 .when(F.col("population_density") > 100, "HIGH")
                 .when(F.col("population_density") > 20, "MEDIUM")
                 .otherwise("LOW"))

        df_fx_t = df_fx \
            .filter(F.col("rate_to_usd") > 0) \
            .withColumn("local_to_usd_rate", F.round(F.lit(1.0) / F.col("rate_to_usd"), 6)) \
            .withColumn("currency_strength",
                F.when(F.col("local_to_usd_rate") >= 1.0, "STRONG")
                 .when(F.col("local_to_usd_rate") >= 0.1, "MODERATE")
                 .when(F.col("local_to_usd_rate") >= 0.01, "WEAK")
                 .otherwise("VERY_WEAK"))

        df_market = df_countries_t.join(
            df_fx_t.select("currency_code", "local_to_usd_rate", "currency_strength", "rate_to_usd"),
            on="currency_code", how="left"
        ).fillna({"local_to_usd_rate": 1.0, "currency_strength": "UNKNOWN"}) \
         .withColumn("logistics_cost_index",
            F.round(F.when(F.col("population_density") > 0,
                           F.lit(1000.0) / F.col("population_density")).otherwise(F.lit(999.0)), 2)) \
         .withColumn("currency_stability_score",
            F.when(F.col("currency_strength") == "STRONG", F.lit(10.0))
             .when(F.col("currency_strength") == "MODERATE", F.lit(7.0))
             .when(F.col("currency_strength") == "WEAK", F.lit(4.0))
             .otherwise(F.lit(2.0))) \
         .withColumn("market_score",
            F.round((F.log1p(F.col("population")) * 0.3) +
                    (F.col("currency_stability_score") * 0.4) +
                    (F.lit(10.0) / (F.col("logistics_cost_index") + 1) * 0.3), 2)) \
         .withColumn("expansion_recommendation",
            F.when(F.col("market_score") >= 8, "EXPAND_NOW")
             .when(F.col("market_score") >= 5, "MONITOR")
             .otherwise("HOLD"))

        df_taxi_t = df_taxi \
            .filter(F.col("fare_amount") > 0) \
            .filter(F.col("trip_distance") > 0) \
            .filter(F.col("fare_amount") < 500) \
            .withColumn("revenue_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 2)) \
            .withColumn("is_profitable", F.when(F.col("fare_amount") >= 5.0, True).otherwise(False)) \
            .withColumn("profitability_tier",
                F.when(F.col("fare_amount") >= 30, "HIGH_VALUE")
                 .when(F.col("fare_amount") >= 10, "STANDARD")
                 .when(F.col("fare_amount") >= 5, "LOW_MARGIN")
                 .otherwise("UNPROFITABLE")) \
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
            .withColumn("pickup_day", F.dayofweek("tpep_pickup_datetime")) \
            .withColumn("pickup_date", F.to_date("tpep_pickup_datetime")) \
            .withColumn("time_period",
                F.when((F.col("pickup_hour") >= 7) & (F.col("pickup_hour") <= 9), "MORNING_RUSH")
                 .when((F.col("pickup_hour") >= 17) & (F.col("pickup_hour") <= 19), "EVENING_RUSH")
                 .when((F.col("pickup_hour") >= 22) | (F.col("pickup_hour") <= 4), "LATE_NIGHT")
                 .when((F.col("pickup_hour") >= 11) & (F.col("pickup_hour") <= 14), "LUNCH_HOUR")
                 .otherwise("OFF_PEAK"))

        df_hourly = df_taxi_t.groupBy("pickup_hour", "time_period") \
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
                F.round(F.avg("revenue_per_mile"), 2).alias("avg_rev_per_mile"),
                F.round(F.sum("fare_amount"), 2).alias("total_revenue_usd"),
                F.round(F.avg(F.col("is_profitable").cast("int")) * 100, 1).alias("profitable_pct")
            )

        df_market.toPandas().to_parquet("data/processed/countries_transformed.parquet", index=False)
        df_fx_t.toPandas().to_parquet("data/processed/fx_rates_transformed.parquet", index=False)
        df_taxi_t.toPandas().to_parquet("data/processed/taxi_transformed.parquet", index=False)
        df_hourly.toPandas().to_parquet("data/processed/taxi_hourly_summary.parquet", index=False)

        spark.stop()
        logger.info("✅ PySpark transforms complete — 4 files saved")
        return "success"

    except Exception as e:
        logger.warning(f"⚠️ Spark unavailable or failed: {e}. Falling back to pandas transforms.")

    try:
        df_countries = pd.read_parquet("data/raw/countries_raw.parquet")
        df_taxi       = pd.read_parquet("data/raw/taxi_raw.parquet")
        df_fx         = pd.read_parquet("data/raw/fx_rates_raw.parquet")

        df_countries = df_countries[df_countries["area_km2"] > 0].copy()
        df_countries["population_density"] = (df_countries["population"] / df_countries["area_km2"]).round(2)
        df_countries["market_size_category"] = pd.cut(
            df_countries["population"],
            bins=[-1, 1_000_000, 10_000_000, 100_000_000, float("inf")],
            labels=["SMALL", "MEDIUM", "LARGE", "MEGA"]
        ).astype(str)
        df_countries["density_category"] = pd.cut(
            df_countries["population_density"],
            bins=[-1, 20, 100, 500, float("inf")],
            labels=["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
        ).astype(str)

        df_fx = df_fx[df_fx["rate_to_usd"] > 0].copy()
        df_fx["local_to_usd_rate"] = (1.0 / df_fx["rate_to_usd"]).round(6)
        df_fx["currency_strength"] = pd.cut(
            df_fx["local_to_usd_rate"],
            bins=[-float("inf"), 0.01, 0.1, 1.0, float("inf")],
            labels=["VERY_WEAK", "WEAK", "MODERATE", "STRONG"]
        ).astype(str)

        df_market = df_countries.merge(
            df_fx[["currency_code", "local_to_usd_rate", "currency_strength", "rate_to_usd"]],
            on="currency_code", how="left"
        )
        df_market["local_to_usd_rate"] = df_market["local_to_usd_rate"].fillna(1.0)
        df_market["currency_strength"] = df_market["currency_strength"].fillna("UNKNOWN")
        df_market["logistics_cost_index"] = df_market["population_density"].apply(
            lambda x: round(1000.0 / x, 2) if x > 0 else 999.0
        )
        df_market["currency_stability_score"] = df_market["currency_strength"].map({
            "STRONG": 10.0,
            "MODERATE": 7.0,
            "WEAK": 4.0,
            "VERY_WEAK": 2.0,
            "UNKNOWN": 2.0
        }).fillna(2.0)
        df_market["market_score"] = (
            (df_market["population"].apply(np.log1p) * 0.3) +
            (df_market["currency_stability_score"] * 0.4) +
            (10.0 / (df_market["logistics_cost_index"] + 1) * 0.3)
        ).round(2)
        df_market["expansion_recommendation"] = pd.cut(
            df_market["market_score"],
            bins=[-float("inf"), 5, 8, float("inf")],
            labels=["HOLD", "MONITOR", "EXPAND_NOW"]
        ).astype(str)

        df_taxi = df_taxi[
            (df_taxi["fare_amount"] > 0) &
            (df_taxi["trip_distance"] > 0) &
            (df_taxi["fare_amount"] < 500)
        ].copy()
        df_taxi["revenue_per_mile"] = (df_taxi["fare_amount"] / df_taxi["trip_distance"]).round(2)
        df_taxi["is_profitable"] = df_taxi["fare_amount"] >= 5.0
        df_taxi["profitability_tier"] = pd.cut(
            df_taxi["fare_amount"],
            bins=[-float("inf"), 5, 10, 30, float("inf")],
            labels=["UNPROFITABLE", "LOW_MARGIN", "STANDARD", "HIGH_VALUE"]
        ).astype(str)
        def map_time_period(hour):
            if 7 <= hour <= 9:
                return "MORNING_RUSH"
            if 17 <= hour <= 19:
                return "EVENING_RUSH"
            if hour >= 22 or hour <= 4:
                return "LATE_NIGHT"
            if 11 <= hour <= 14:
                return "LUNCH_HOUR"
            return "OFF_PEAK"

        df_taxi["pickup_hour"] = pd.to_datetime(df_taxi["tpep_pickup_datetime"]).dt.hour
        df_taxi["pickup_day"] = pd.to_datetime(df_taxi["tpep_pickup_datetime"]).dt.dayofweek + 1
        df_taxi["pickup_date"] = pd.to_datetime(df_taxi["tpep_pickup_datetime"]).dt.date
        df_taxi["time_period"] = df_taxi["pickup_hour"].apply(map_time_period)

        hourly = df_taxi.groupby(["pickup_hour", "time_period"])
        df_hourly = hourly.agg(
            trip_count=("fare_amount", "count"),
            avg_fare_usd=("fare_amount", lambda x: round(x.mean(), 2)),
            avg_rev_per_mile=("revenue_per_mile", lambda x: round(x.mean(), 2)),
            total_revenue_usd=("fare_amount", lambda x: round(x.sum(), 2)),
            profitable_pct=("is_profitable", lambda x: round(100.0 * x.astype(int).mean(), 1))
        ).reset_index()

        df_market.to_parquet("data/processed/countries_transformed.parquet", index=False)
        df_fx.to_parquet("data/processed/fx_rates_transformed.parquet", index=False)
        df_taxi.to_parquet("data/processed/taxi_transformed.parquet", index=False)
        df_hourly.to_parquet("data/processed/taxi_hourly_summary.parquet", index=False)

        logger.info("✅ Pandas fallback transforms complete — 4 files saved")
        return "success"

    except Exception as e:
        logger.error(f"❌ Transform failed: {e}")
        raise


# ══════════════════════════════════════════════════════════════════
#  TASK 3 — LOAD
# ══════════════════════════════════════════════════════════════════

@task(name="Load into DuckDB", retries=2)
def load_to_duckdb():
    logger = get_run_logger()
    logger.info("🦆 Loading into DuckDB...")

    os.makedirs("data", exist_ok=True)
    con = duckdb.connect(DB_PATH)

    # Drop existing objects before recreating them
    objects_to_drop = [
        "vw_market_overview", "vw_profitability", "vw_regional_revenue",
        "countries", "fx_rates", "taxi_trips", "taxi_hourly"
    ]
    
    for obj in objects_to_drop:
        try:
            con.execute(f"DROP VIEW IF EXISTS {obj}")
        except:
            pass  # View doesn't exist or is actually a table
        try:
            con.execute(f"DROP TABLE IF EXISTS {obj}")
        except:
            pass  # Table doesn't exist

    con.execute("CREATE TABLE countries AS SELECT * FROM read_parquet('data/processed/countries_transformed.parquet')")
    con.execute("CREATE TABLE fx_rates AS SELECT * FROM read_parquet('data/processed/fx_rates_transformed.parquet')")
    con.execute("CREATE TABLE taxi_trips AS SELECT * FROM read_parquet('data/processed/taxi_transformed.parquet')")
    con.execute("CREATE TABLE taxi_hourly AS SELECT * FROM read_parquet('data/processed/taxi_hourly_summary.parquet')")

    # Create views
    con.execute("""
        CREATE VIEW vw_market_overview AS
        SELECT region,
               COUNT(*) AS num_countries,
               SUM(population) AS total_population,
               ROUND(AVG(population_density), 2) AS avg_density,
               ROUND(AVG(market_score), 2) AS avg_market_score,
               COUNT(CASE WHEN expansion_recommendation = 'EXPAND_NOW' THEN 1 END) AS expand_now_count,
               COUNT(CASE WHEN currency_strength IN ('STRONG','MODERATE') THEN 1 END) AS stable_currency_count
        FROM countries GROUP BY region ORDER BY avg_market_score DESC
    """)

    con.execute("""
        CREATE VIEW vw_profitability AS
        SELECT time_period, pickup_hour,
               COUNT(*) AS total_trips,
               ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
               ROUND(AVG(revenue_per_mile), 2) AS avg_rev_per_mile,
               ROUND(SUM(fare_amount), 2) AS total_revenue_usd,
               ROUND(SUM(tip_amount), 2) AS total_tips_usd,
               COUNT(CASE WHEN is_profitable = true THEN 1 END) AS profitable_trips,
               ROUND(COUNT(CASE WHEN is_profitable = true THEN 1 END) * 100.0 / COUNT(*), 1) AS profitable_pct
        FROM taxi_trips GROUP BY time_period, pickup_hour ORDER BY pickup_hour
    """)

    con.execute("""
        CREATE VIEW vw_regional_revenue AS
        SELECT c.region, c.country_name, c.currency_code, c.currency_strength,
               c.population, c.population_density, c.market_score,
               c.expansion_recommendation, f.rate_to_usd, f.local_to_usd_rate,
               ROUND(15.50 * f.rate_to_usd, 2) AS avg_fare_in_local_currency,
               ROUND(c.population * 0.001 * 15.50, 0) AS estimated_annual_revenue_usd
        FROM countries c LEFT JOIN fx_rates f ON c.currency_code = f.currency_code
        ORDER BY estimated_annual_revenue_usd DESC
    """)

    # Log counts
    for t in ["countries","fx_rates","taxi_trips","taxi_hourly"]:
        cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        logger.info(f"  📊 {t}: {cnt:,} rows")

    con.close()
    logger.info("✅ DuckDB load complete")
    return "success"


# ══════════════════════════════════════════════════════════════════
#  TASK 4 — VALIDATE
# ══════════════════════════════════════════════════════════════════

@task(name="Validate Pipeline Output")
def validate():
    logger = get_run_logger()
    logger.info("🔍 Validating pipeline output...")

    con = duckdb.connect(DB_PATH, read_only=True)

    checks = {
        "countries table":      "SELECT COUNT(*) FROM countries",
        "fx_rates table":       "SELECT COUNT(*) FROM fx_rates",
        "taxi_trips table":     "SELECT COUNT(*) FROM taxi_trips",
        "market_overview view": "SELECT COUNT(*) FROM vw_market_overview",
        "profitability view":   "SELECT COUNT(*) FROM vw_profitability",
    }

    all_pass = True
    for name, sql in checks.items():
        count = con.execute(sql).fetchone()[0]
        status = "✅" if count > 0 else "❌"
        logger.info(f"  {status} {name}: {count:,} rows")
        if count == 0:
            all_pass = False

    con.close()

    if not all_pass:
        raise Exception("❌ Validation failed — some tables are empty!")

    logger.info("✅ All validation checks passed!")
    return "validated"


# ══════════════════════════════════════════════════════════════════
#  MAIN FLOW
# ══════════════════════════════════════════════════════════════════

@flow(
    name="Global Logistics ETL Pipeline",
    description="Extracts country, FX, and taxi data → Transforms with PySpark → Loads to DuckDB"
)
def etl_pipeline():
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("🚀 GLOBAL LOGISTICS ETL PIPELINE STARTED")
    logger.info(f"   Start time: {start_time}")
    logger.info("=" * 60)

    # ── EXTRACT (parallel-ready) ──────────────────────────────────
    logger.info("\n📥 STAGE 1: EXTRACT")
    n_countries = extract_countries()
    n_fx        = extract_fx_rates()
    n_taxi      = extract_taxi()
    logger.info(f"   Extracted: {n_countries} countries, {n_fx} FX rates, {n_taxi:,} taxi trips")

    # ── TRANSFORM ─────────────────────────────────────────────────
    logger.info("\n⚡ STAGE 2: TRANSFORM (PySpark)")
    spark_result = transform_with_spark()
    logger.info(f"   Transform result: {spark_result}")

    # ── LOAD ──────────────────────────────────────────────────────
    logger.info("\n🦆 STAGE 3: LOAD (DuckDB)")
    load_result = load_to_duckdb()
    logger.info(f"   Load result: {load_result}")

    # ── VALIDATE ──────────────────────────────────────────────────
    logger.info("\n🔍 STAGE 4: VALIDATE")
    val_result = validate()
    logger.info(f"   Validation: {val_result}")

    # ── DONE ──────────────────────────────────────────────────────
    duration = (datetime.now() - start_time).seconds
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ PIPELINE COMPLETE in {duration}s")
    logger.info(f"   Dashboard: streamlit run 05_dashboard.py")
    logger.info("=" * 60)


# ── RUN ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\nStarting ETL Pipeline...")
    print("   To monitor: prefect server start -> http://localhost:4200\n")
    etl_pipeline()

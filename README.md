# 🚀 Global Logistics Analytics — ETL Pipeline

A production-grade data pipeline that extracts global market data, transforms it with Apache PySpark, loads it into DuckDB, and visualizes insights through an interactive Streamlit dashboard.

---

## 📌 Business Problem

> **"Which global markets offer the best logistics ROI, and which taxi trip time slots are unprofitable?"**

---

## 🏗️ Architecture Diagram

```
┌──────────────────────────────────────────────────────────┐
│               PREFECT ORCHESTRATION                       │
│                                                           │
│  EXTRACT          TRANSFORM          LOAD                 │
│                                                           │
│  🌍 Countries  ─▶                                        │
│  💱 FX Rates   ─▶  ⚡ PySpark   ─▶  🦆 DuckDB  ─▶ 📊   │
│  🚕 Taxi .parq ─▶                        │        Streamlit│
│                                           ▼               │
│                                     Views + Tables        │
└──────────────────────────────────────────────────────────┘
```

---

## 📊 Data Sources

| # | Source | Type | What We Extract |
|---|--------|------|-----------------|
| 1 | REST Countries API | REST API | 250 countries: population, area, currency |
| 2 | Open Exchange Rates | REST API | Live USD rates for 170 currencies |
| 3 | NYC Taxi (TLC) | Parquet | Millions of real trip records |

---

## ⚡ Transformations With Business Impact

| Transform | Formula | Impact |
|-----------|---------|--------|
| Population Density | population / area_km2 | Dense cities = efficient delivery |
| Currency Normalize | 1 / rate_to_usd | True USD revenue across 170 markets |
| Trip Profitability | fare >= $5.00 | Found 20%+ below-threshold trips |
| Peak Hours | Hour buckets | 2-4am least profitable — needs surge pricing |
| Market ROI Score | Weighted formula | Ranked 250 markets for expansion |

---

## ▶️ How to Run

### Full Pipeline (Recommended)
```bash
python 06_orchestration.py
```

### Step by Step (Notebooks)
```
Run in order: 01 → 02 → 03 → 04
```

### Launch Dashboard
```bash
streamlit run 05_dashboard.py
# Opens at http://localhost:8501
```

### Monitor Prefect
```bash
prefect server start   # http://localhost:4200
python 06_orchestration.py
```

---

## 📁 Project Structure

```
├── 01_extract_countries.ipynb   # Andarge seifu
├── 02_extract_taxi_fx.ipynb     # bereket Andualem
├── 03_transform_spark.ipynb     # Bisrat engda
├── 04_load_duckdb.ipynb         # Afomia kelemwork
├── 05_dashboard.py              # meklit legese
├── 06_orchestration.py          # meron  takele
├── data/
│   ├── raw/
│   └── processed/
└── README.md
```

---

## 👥 Team Contributions

| Member | File | Responsibility |
|--------|------|---------------|
| Andarge seifu | `01_extract_countries.ipynb` | REST Countries API, JSON parsing, Parquet save |
| bereket Andualem | `02_extract_taxi_fx.ipynb` | NYC Taxi Parquet, Exchange Rates API |
| Bisrat engda | `03_transform_spark.ipynb` | All PySpark transforms + business logic |
| Afomia kelemwork | `04_load_duckdb.ipynb` | DuckDB tables, views, SQL queries |
| meklit legese| `05_dashboard.py` | Streamlit dashboard, charts, filters |
| meron  takele | `06_orchestration.py` | Prefect flow, task dependencies, README |

---

## 📦 Install Dependencies

```bash
pip install pyspark duckdb pandas pyarrow requests streamlit plotly prefect jupyter ipykernel
```

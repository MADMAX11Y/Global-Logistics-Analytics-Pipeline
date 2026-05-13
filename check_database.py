"""
Quick Database Inspection Script
Run: python check_database.py
"""

import duckdb
from pathlib import Path

DB_PATH = 'data/pipeline_analytics.duckdb'

def check_database():
    """Inspect DuckDB database state and contents."""
    
    if not Path(DB_PATH).exists():
        print(f"❌ Database not found: {DB_PATH}")
        return
    
    con = duckdb.connect(DB_PATH, read_only=True)
    
    print("\n" + "="*70)
    print("🦆 DUCKDB DATABASE STATE")
    print("="*70)
    
    # List all tables and views
    print("\n📋 SCHEMA:")
    schema = con.execute("""
        SELECT table_name, table_type 
        FROM information_schema.tables 
        ORDER BY table_type DESC, table_name
    """).fetchall()
    
    for name, ttype in schema:
        print(f"\n  {ttype}: {name}")
        
        # Get row count for tables
        if ttype == 'BASE TABLE':
            count = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"    Rows: {count:,}")
        
        # Get columns
        cols = con.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{name}' 
            ORDER BY ordinal_position
        """).fetchall()
        
        print(f"    Columns: {len(cols)}")
        for col, dtype in cols[:5]:  # Show first 5 columns
            print(f"      • {col}: {dtype}")
        if len(cols) > 5:
            print(f"      ... and {len(cols) - 5} more")
    
    # Quick stats
    print("\n" + "="*70)
    print("📊 STATISTICS:")
    print("="*70)
    
    countries_count = con.execute("SELECT COUNT(*) FROM countries").fetchone()[0]
    fx_count = con.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
    taxi_count = con.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
    regions_count = con.execute("SELECT COUNT(DISTINCT region) FROM countries").fetchone()[0]
    
    print(f"\n  Countries: {countries_count:,} in {regions_count} regions")
    print(f"  Exchange Rates: {fx_count:,} currencies")
    print(f"  Taxi Trips: {taxi_count:,} records")
    
    # View summaries
    print("\n" + "="*70)
    print("📈 VIEW SUMMARIES:")
    print("="*70)
    
    print("\n  Market Overview by Region:")
    df = con.execute("""
        SELECT region, num_countries, total_population, avg_market_score, expand_now_count
        FROM vw_market_overview
        ORDER BY avg_market_score DESC
    """).df()
    for _, row in df.iterrows():
        print(f"    • {row['region']}: {row['num_countries']} countries, {row['expand_now_count']} for expansion")
    
    print("\n  Taxi Profitability by Time Period:")
    df = con.execute("""
        SELECT time_period, total_trips, avg_fare_usd, profitable_pct
        FROM vw_profitability
        ORDER BY pickup_hour
    """).df()
    for _, row in df.iterrows():
        print(f"    • {row['time_period']}: {row['total_trips']:,} trips, avg ${row['avg_fare_usd']:.2f}, {row['profitable_pct']:.1f}% profitable")
    
    con.close()
    print("\n" + "="*70)
    print("✅ Database is healthy and ready to use!")
    print("="*70 + "\n")

if __name__ == "__main__":
    check_database()

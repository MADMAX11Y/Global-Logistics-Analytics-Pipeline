"""
Member 5 — Streamlit Dashboard
Global Logistics Analytics Dashboard
Run: streamlit run 05_dashboard.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# ── DATABASE PATH ──────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent
possible_db_paths = [
    ROOT_DIR.parent / 'data' / 'pipeline_analytics.duckdb',
    ROOT_DIR / 'data' / 'pipeline_analytics.duckdb',
    ROOT_DIR.parent / 'notebooks' / 'data' / 'pipeline_analytics.duckdb',
]
DB_PATH = None
for candidate in possible_db_paths:
    if candidate.exists():
        DB_PATH = candidate
        break

if DB_PATH is None:
    DB_PATH = ROOT_DIR.parent / 'data' / 'pipeline_analytics.duckdb'
    DB_PATH = str(DB_PATH)
else:
    DB_PATH = str(DB_PATH)

# ── PAGE CONFIG ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Global Logistics Analytics",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── CUSTOM CSS ────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f, #0d2137);
        border-radius: 12px;
        padding: 20px;
        border-left: 4px solid #00d4ff;
        margin-bottom: 10px;
    }
    .insight-box {
        background: #1a1a2e;
        border-radius: 8px;
        padding: 15px;
        border-left: 4px solid #ff6b35;
        margin: 10px 0;
    }
    h1 { color: #00d4ff !important; }
    .stMetric label { color: #a0aec0 !important; }
</style>
""", unsafe_allow_html=True)

# ── DATABASE CONNECTION ───────────────────────────────────────────
@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data(ttl=300)
def query(sql):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(sql).df()
    con.close()
    return df

# ── HEADER ────────────────────────────────────────────────────────
st.markdown("# 🚀 Global Logistics Analytics Pipeline")
st.markdown("**Powered by:** PySpark · DuckDB · Prefect · Streamlit")
st.markdown("---")

# ── SIDEBAR FILTERS ───────────────────────────────────────────────
st.sidebar.markdown("## 🎛️ Filters")

regions = query("SELECT DISTINCT region FROM countries ORDER BY region")['region'].tolist()
selected_regions = st.sidebar.multiselect(
    "Select Regions", regions, default=regions
)

expansion_filter = st.sidebar.selectbox(
    "Expansion Status",
    ["All", "EXPAND_NOW", "MONITOR", "HOLD"]
)

hour_range = st.sidebar.slider(
    "Pickup Hour Range", 0, 23, (0, 23)
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Pipeline Info")
st.sidebar.markdown("**Sources:** 3 (API × 2, Parquet × 1)")
st.sidebar.markdown("**Transform:** PySpark")
st.sidebar.markdown("**Storage:** DuckDB")
st.sidebar.markdown("**Orchestration:** Prefect")

# ── LOAD DATA ─────────────────────────────────────────────────────
try:
    df_market    = query("SELECT * FROM vw_market_overview")
    df_countries = query("SELECT * FROM countries")
    df_profit    = query("SELECT * FROM vw_profitability ORDER BY pickup_hour")
    df_regional  = query("SELECT * FROM vw_regional_revenue")
    df_fx        = query("SELECT * FROM fx_rates ORDER BY rate_to_usd DESC")

    # Apply region filter
    if selected_regions:
        df_countries  = df_countries[df_countries['region'].isin(selected_regions)]
        df_regional   = df_regional[df_regional['region'].isin(selected_regions)]

    if expansion_filter != "All":
        df_countries = df_countries[df_countries['expansion_recommendation'] == expansion_filter]

    # Apply hour filter
    df_profit_filtered = df_profit[
        (df_profit['pickup_hour'] >= hour_range[0]) &
        (df_profit['pickup_hour'] <= hour_range[1])
    ]

except Exception as e:
    st.error(f"❌ Database error: {e}")
    st.info("Make sure you have run notebooks 01 → 04 first!")
    st.stop()

# ── KPI METRICS ROW ───────────────────────────────────────────────
st.markdown("## 📊 Key Performance Indicators")

col1, col2, col3, col4, col5 = st.columns(5)

total_countries = len(df_countries)
expand_now      = len(df_countries[df_countries['expansion_recommendation'] == 'EXPAND_NOW'])
total_trips     = int(df_profit['total_trips'].sum())
total_revenue   = df_profit['total_revenue_usd'].sum()
avg_profitable  = df_profit['profitable_pct'].mean()

col1.metric("🌍 Countries Analyzed", f"{total_countries}")
col2.metric("🚀 Expand Now Markets", f"{expand_now}")
col3.metric("🚕 Total Trips", f"{total_trips:,}")
col4.metric("💰 Total Revenue", f"${total_revenue:,.0f}")
col5.metric("📈 Avg Profitable %", f"{avg_profitable:.1f}%")

st.markdown("---")

# ── ROW 1: MAP + MARKET SCORE ─────────────────────────────────────
st.markdown("## 🌍 Global Market Analysis")

col_map, col_bar = st.columns([2, 1])

with col_map:
    st.markdown("### Market Score by Country")
    fig_map = px.choropleth(
        df_countries,
        locations='country_name',
        locationmode='country names',
        color='market_score',
        hover_name='country_name',
        hover_data=['region', 'expansion_recommendation', 'currency_strength', 'population_density'],
        color_continuous_scale='RdYlGn',
        title='Global Market Opportunity Score',
        template='plotly_dark'
    )
    fig_map.update_layout(height=400, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_map, width='stretch')

with col_bar:
    st.markdown("### Regional Market Score")
    df_market_sorted = df_market.sort_values('avg_market_score', ascending=True)
    fig_bar = px.bar(
        df_market_sorted,
        x='avg_market_score',
        y='region',
        orientation='h',
        color='avg_market_score',
        color_continuous_scale='Blues',
        title='Avg Market Score by Region',
        template='plotly_dark'
    )
    fig_bar.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_bar, width='stretch')

# ── ROW 2: TAXI PROFITABILITY ─────────────────────────────────────
st.markdown("---")
st.markdown("## 🚕 Trip Profitability Analysis")

col_line, col_pie = st.columns([2, 1])

with col_line:
    st.markdown("### Revenue & Profitability by Hour")
    fig_dual = make_subplots(specs=[[{"secondary_y": True}]])

    fig_dual.add_trace(
        go.Bar(
            x=df_profit_filtered['pickup_hour'],
            y=df_profit_filtered['total_revenue_usd'],
            name='Total Revenue ($)',
            marker_color='#00d4ff',
            opacity=0.7
        ),
        secondary_y=False
    )
    fig_dual.add_trace(
        go.Scatter(
            x=df_profit_filtered['pickup_hour'],
            y=df_profit_filtered['profitable_pct'],
            name='Profitable %',
            line=dict(color='#ff6b35', width=3),
            mode='lines+markers'
        ),
        secondary_y=True
    )
    fig_dual.update_layout(
        title='Hourly Revenue vs Profitability Rate',
        template='plotly_dark',
        height=380
    )
    fig_dual.update_yaxes(title_text="Revenue (USD)", secondary_y=False)
    fig_dual.update_yaxes(title_text="Profitable %", secondary_y=True)
    st.plotly_chart(fig_dual, width='stretch')

with col_pie:
    st.markdown("### Time Period Distribution")
    df_period = df_profit.groupby('time_period').agg(
        total_revenue=('total_revenue_usd','sum'),
        total_trips=('total_trips','sum')
    ).reset_index()

    fig_pie = px.pie(
        df_period,
        values='total_revenue',
        names='time_period',
        title='Revenue Share by Time Period',
        color_discrete_sequence=px.colors.qualitative.Set3,
        template='plotly_dark'
    )
    fig_pie.update_layout(height=380)
    st.plotly_chart(fig_pie, width='stretch')

# ── KEY INSIGHT BOX ───────────────────────────────────────────────
worst_hour = df_profit.loc[df_profit['profitable_pct'].idxmin()]
best_hour  = df_profit.loc[df_profit['profitable_pct'].idxmax()]

st.markdown(f"""
<div class="insight-box">
    <b>💡 KEY INSIGHT — Surge Pricing Opportunity:</b><br>
    Hour <b>{int(worst_hour['pickup_hour'])}:00</b> has only 
    <b>{worst_hour['profitable_pct']:.1f}%</b> profitable trips 
    (time period: {worst_hour['time_period']}) — 
    Surge pricing should activate here.<br>
    Best hour is <b>{int(best_hour['pickup_hour'])}:00</b> with 
    <b>{best_hour['profitable_pct']:.1f}%</b> profitable trips 
    generating ${best_hour['total_revenue_usd']:,.0f} revenue.
</div>
""", unsafe_allow_html=True)

# ── ROW 3: CURRENCY ANALYSIS ──────────────────────────────────────
st.markdown("---")
st.markdown("## 💱 Currency & Market Intelligence")

col_fx, col_expand = st.columns(2)

with col_fx:
    st.markdown("### Currency Strength Distribution")
    df_cs = df_countries.groupby(['region', 'currency_strength']).size().reset_index(name='count')
    fig_cs = px.bar(
        df_cs,
        x='region',
        y='count',
        color='currency_strength',
        title='Currency Strength by Region',
        barmode='stack',
        color_discrete_map={
            'STRONG': '#00c851',
            'MODERATE': '#ffbb33',
            'WEAK': '#ff4444',
            'VERY_WEAK': '#cc0000',
            'UNKNOWN': '#666666'
        },
        template='plotly_dark'
    )
    fig_cs.update_layout(height=380, xaxis_tickangle=-30)
    st.plotly_chart(fig_cs, width='stretch')

with col_expand:
    st.markdown("### Expansion Recommendations")
    df_exp = df_countries.groupby(['region', 'expansion_recommendation']).size().reset_index(name='count')
    fig_exp = px.bar(
        df_exp,
        x='region',
        y='count',
        color='expansion_recommendation',
        title='Market Expansion Status by Region',
        barmode='group',
        color_discrete_map={
            'EXPAND_NOW': '#00c851',
            'MONITOR': '#ffbb33',
            'HOLD': '#ff4444'
        },
        template='plotly_dark'
    )
    fig_exp.update_layout(height=380, xaxis_tickangle=-30)
    st.plotly_chart(fig_exp, width='stretch')

# ── ROW 4: DATA TABLE ─────────────────────────────────────────────
st.markdown("---")
st.markdown("## 📋 Country-Level Detail")

display_cols = [
    'country_name', 'region', 'population', 'population_density',
    'currency_code', 'currency_strength', 'market_score', 'expansion_recommendation'
]
available = [c for c in display_cols if c in df_countries.columns]
st.dataframe(
    df_countries[available].sort_values('market_score', ascending=False),
    width='stretch',
    height=400
)

# ── FOOTER ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
**🏗️ Pipeline Architecture:** `REST Countries API` + `Exchange Rates API` + `NYC Taxi Parquet`
→ **PySpark Transforms** → **DuckDB** → **This Dashboard**
""")

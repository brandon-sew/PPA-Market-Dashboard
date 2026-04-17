import streamlit as st
import pandas as pd
import plotly.express as px
import os
import json
import glob
import numpy as np
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Config & API Setup
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

ZONE_NAMES = {
    "AT": ["Austria", "EUR"], "BE": ["Belgium", "EUR"], "BG": ["Bulgaria", "EUR"],
    "CH": ["Switzerland", "EUR"], "CZ": ["Czech Republic", "EUR"], 
    "DE_LU": ["Germany & Luxembourg", "EUR"], "FR": ["France", "EUR"], 
    "GB": ["Great Britain", "GBP"], "IE_SEM": ["Ireland", "EUR"],
    "NL": ["Netherlands", "EUR"], "PL": ["Poland", "PLN"], 
    "DK_1": ["Denmark 1", "EUR"], "DK_2": ["Denmark 2", "EUR"],
    "EE": ["Estonia", "EUR"], "FI": ["Finland", "EUR"], "LT": ["Lithuania", "EUR"],
    "LV": ["Latvia", "EUR"], "NO_1": ["Norway 1", "EUR"], "NO_2": ["Norway 2", "EUR"],
    "NO_3": ["Norway 3", "EUR"], "NO_4": ["Norway 4", "EUR"], "NO_5": ["Norway 5", "EUR"],
    "SE_1": ["Sweden 1", "EUR"], "SE_2": ["Sweden 2", "EUR"], "SE_3": ["Sweden 3", "EUR"],
    "SE_4": ["Sweden 4", "EUR"], "ES": ["Spain", "EUR"], "PT": ["Portugal", "EUR"],
    "IT_NORD": ["Italy North", "EUR"], "IT_CNOR": ["Italy C-North", "EUR"],
    "IT_CSUD": ["Italy C-South", "EUR"], "IT_SUD": ["Italy South", "EUR"],
    "IT_SICI": ["Sicily", "EUR"], "IT_SARD": ["Sardinia", "EUR"]
}

st.set_page_config(page_title="Market Explorer", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    section[data-testid="stSidebar"] { width: 400px !important; }
    .main .block-container { padding-top: 2rem !important; max-width: 98% !important; }
    </style>
    """, unsafe_allow_html=True)

if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

# --- SIDEBAR ---
with st.sidebar:
    st.title("Configuration")
    display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}
    st.multiselect("Select bidding zones:", options=sorted(display_options.keys()), key="selected_zones")
    st.divider()
    res = st.radio("Resolution", ["60 min", "15 min"], horizontal=True)
    today = datetime.now().date()
    d_range = st.date_input("Date Range", value=(today - timedelta(days=2), today))

# --- DATA FETCHING ---
@st.cache_data(ttl=3600)
def fetch_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    all_data = []
    for code in codes:
        try:
            series = client.query_day_ahead_prices(code, start=start, end=end)
            df = series.to_frame(name='Price').reset_index()
            df.columns = ['Time', 'Price']
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            all_data.append(df)
        except: continue
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_gen_data(codes, start_date, end_date, freq):
    if not codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    all_gen = []
    for code in codes:
        try:
            df = client.query_generation(code, start=start, end=end)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.groupby(df.columns, axis=1).sum()
            df = df.resample(freq).mean().ffill()
            df = df.reset_index().rename(columns={'index': 'Time'})
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            all_gen.append(df)
        except: continue
    return pd.concat(all_gen, ignore_index=True) if all_gen else pd.DataFrame()

# --- MAIN LOGIC ---
st.title("⚡ European Energy Market Explorer")
selected_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
plot_df = pd.DataFrame()
full_price_df = pd.DataFrame()

if len(d_range) == 2:
    freq = '60min' if res == "60 min" else '15min'
    full_price_df = fetch_data(list(ZONE_NAMES.keys()), d_range[0], d_range[1])
    gen_df = fetch_gen_data(selected_codes, d_range[0], d_range[1], freq)
    
    if not full_price_df.empty:
        full_price_resampled = full_price_df.groupby('Zone').apply(
            lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()
        ).reset_index()
        plot_df = full_price_resampled[full_price_resampled['Zone'].isin(selected_codes)].copy()
        plot_df['Currency'] = plot_df['Zone'].apply(lambda x: ZONE_NAMES.get(x, ['', 'EUR'])[1])
        plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES.get(x, ['', 'EUR'])[1]}/MWh)")

# --- MIDDLE SECTION ---
col_chart, col_map = st.columns([2, 1])
with col_chart:
    st.subheader("Day-Ahead Prices")
    if not plot_df.empty:
        fig_line = px.line(plot_df, x='Time', y='Price', color='Display', template="plotly_white", custom_data=['Currency'])
        fig_line.update_layout(legend=dict(orientation="h", y=-0.2), hoverlabel=dict(bgcolor="white", font_size=18))
        st.plotly_chart(fig_line, use_container_width=True)

with col_map:
    # (Map code remains identical to previous version, ensuring it uses full_price_resampled)
    pass # [Assumes map code provided previously remains here]

# --- METRICS SECTION ---
st.divider()
col_met, col_tab = st.columns([1, 2])

with col_met:
    st.subheader("Baseload & Capture Metrics")
    if not plot_df.empty and not gen_df.empty:
        metrics_list = []
        for code in selected_codes:
            p_sub = plot_df[plot_df['Zone'] == code].sort_values('Time')
            g_sub = gen_df[gen_df['Zone'] == code].sort_values('Time')
            
            # FIX: Use merge_asof to handle small timestamp drifts between datasets
            m_df = pd.merge_asof(p_sub, g_sub, on='Time', direction='nearest')
            
            baseload = p_sub['Price'].mean()
            currency = ZONE_NAMES[code][1]
            sol_cap = "N/A"
            if 'Solar' in m_df.columns:
                total_sol = m_df['Solar'].sum()
                if total_sol > 0:
                    sol_cap = f"{(m_df['Price'] * m_df['Solar']).sum() / total_sol:.2f}"
            
            wind_cols = [c for c in ['Wind Onshore', 'Wind Offshore'] if c in m_df.columns]
            wind_cap = "N/A"
            if wind_cols:
                total_wind = m_df[wind_cols].sum(axis=1).sum()
                if total_wind > 0:
                    wind_cap = f"{(m_df['Price'] * m_df[wind_cols].sum(axis=1)).sum() / total_wind:.2f}"
            
            metrics_list.append({"Zone": code, "Baseload": f"{baseload:.2f}", "Solar Capture": sol_cap, "Wind Capture": wind_cap, "Unit": f"{currency}/MWh"})
        st.table(pd.DataFrame(metrics_list))

with col_tab:
    st.subheader("Hourly Price Data")
    if not plot_df.empty:
        # Create a clean pivot for multiple countries
        pivot_df = plot_df.copy()
        pivot_df['Time_Label'] = pivot_df['Time'].dt.strftime('%d-%m %H:%M')
        table_pivot = pivot_df.pivot(index='Time_Label', columns='Display', values='Price')
        st.dataframe(table_pivot.style.format("{:.2f}"), use_container_width=True, height=400)

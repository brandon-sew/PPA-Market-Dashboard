import streamlit as st
import pandas as pd
import plotly.express as px
import os
import json
import glob
import numpy as np
import requests
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from concurrent.futures import ThreadPoolExecutor, as_completed
import feedparser

# 1. Config & API Setup
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

ZONE_NAMES = {
    "AT": ["Austria", "EUR"], "BE": ["Belgium", "EUR"], "BG": ["Bulgaria", "EUR"],
    "CH": ["Switzerland", "EUR"], "CZ": ["Czech Republic", "EUR"], 
    "DE_LU": ["Germany & Luxembourg", "EUR"], "FR": ["France", "EUR"], 
    "GB": ["Great Britain", "GBP"], "IE_SEM": ["Ireland", "EUR"],
    "NL": ["Netherlands", "EUR"], "PL": ["Poland", "PLN"], 
    "DK_1": ["Denmark West", "EUR"], "DK_2": ["Denmark East", "EUR"],
    "EE": ["Estonia", "EUR"], "FI": ["Finland", "EUR"], "LT": ["Lithuania", "EUR"],
    "LV": ["Latvia", "EUR"], "NO_1": ["Norway East", "EUR"], "NO_2": ["Norway South", "EUR"],
    "NO_3": ["Norway Central", "EUR"], "NO_4": ["Norway Northern", "EUR"], "NO_5": ["Norway West", "EUR"],
    "SE_1": ["Sweden Luleå", "EUR"], "SE_2": ["Sweden Sundsvall", "EUR"], "SE_3": ["Sweden Stockholm", "EUR"],
    "SE_4": ["Sweden Malmö", "EUR"], "ES": ["Spain", "EUR"], "PT": ["Portugal", "EUR"],
    "HR": ["Croatia", "EUR"], "HU": ["Hungary", "EUR"], 
    "ME": ["Montenegro", "EUR"], "MK": ["North Macedonia", "EUR"],
    "RO": ["Romania", "EUR"], "RS": ["Serbia", "EUR"], 
    "SI": ["Slovenia", "EUR"], "SK": ["Slovakia", "EUR"],
    "IT_NORD": ["Italy North", "EUR"], "IT_CNOR": ["Italy Central North", "EUR"],
    "IT_CSUD": ["Italy Central South", "EUR"], "IT_SUD": ["Italy South", "EUR"],
    "IT_SICI": ["Italy Sicily", "EUR"], "IT_SARD": ["Italy Sardinia", "EUR"], "IT_CALA": ["Italy Calabria", "EUR"]
}

st.set_page_config(page_title="Market Explorer", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    section[data-testid="stSidebar"] { width: 400px !important; }
    .main .block-container { 
        padding-top: 2rem !important;
        max-width: 98% !important; 
    }
    </style>
    """, unsafe_allow_html=True)

if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

with st.sidebar:
    st.title("Configuration")
    display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}
    
    chosen_from_dropdown = st.multiselect("Select bidding zones:", 
                                         options=sorted(display_options.keys()), 
                                         default=st.session_state.selected_zones)
    
    if chosen_from_dropdown != st.session_state.selected_zones:
        st.session_state.selected_zones = chosen_from_dropdown
        st.rerun()
    
    gen_options = ["Solar", "Wind Onshore", "Wind Offshore"]
    selected_gen_types = st.multiselect("Overlay Generation Forecast:", options=gen_options, key="gen_forecast_select")
    res = st.radio("Resolution", ["Monthly", "Daily", "60 min", "15 min"], horizontal=True, key="res_radio")
    today = datetime.now().date()
    d_range = st.date_input("Date Range", value=(today - timedelta(days=2), today), key="date_range_input")
    exclude_neg = st.checkbox("No Settlement for Negative Prices", help="Treats negative prices as 0 for capture price calculation", key="neg_price_check")

    st.divider()
    st.subheader("PPA Configuration")
    ppa_price = st.number_input("PPA Price (EUR/MWh)", value=0.0, step = 1.0, key="ppa_price_input")
    fixed_floating = st.checkbox("Fixed for Floating Price Structure", key="fixed_float_check")
    market_following = st.checkbox("Market Following with floor", key="mkt_follow_check")
    if market_following:
        floor_rate_eur = st.number_input("Floor Rate (EUR/MWh)", value=0.0, step=0.1, key="floor_eur_input")
        floor_rate_pct = st.number_input("Floor Rate (% of PPA Price)", value=0.0, step=1.0, key="floor_pct_input")
        if floor_rate_eur > 0 and floor_rate_pct > 0:
            st.error("⚠️ Please specify either a Floor Rate (EUR) OR a Floor Rate (%), not both.")
    
    if (fixed_floating or market_following) and res != "Monthly":
        st.error("⚠️ Settlement not available on a Daily/60min/15min basis, please select Monthly.")

@st.cache_data
def load_local_csv():
    if os.path.exists('market_prices.csv'):
        df = pd.read_csv('market_prices.csv')
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        return df
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start, end = pd.Timestamp(start_date, tz='Europe/Brussels'), pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    def get_price(code):
        try:
            series = client.query_day_ahead_prices(code, start=start, end=end)
            df = series.to_frame(name='Price').reset_index()
            df.columns = ['Time', 'Price']
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            return df
        except: return None
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_price, code) for code in codes]
        for future in as_completed(futures):
            res_data = future.result(); 
            if res_data is not None: all_data.append(res_data)
    return pd.concat(all_data) if all_data else pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_gen_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start, end = pd.Timestamp(start_date, tz='Europe/Brussels'), pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    def get_gen(code):
        try:
            df = client.query_generation(code, start=start, end=end)
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            df = df.T.groupby(level=0).sum().T.reset_index().rename(columns={'index': 'Time'})
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            return df
        except: return None
    all_gen = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_gen, code) for code in codes]
        for future in as_completed(futures):
            res_data = future.result(); 
            if res_data is not None: all_gen.append(res_data)
    return pd.concat(all_gen) if all_gen else pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_forecast_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start, end = pd.Timestamp(start_date, tz='Europe/Brussels'), pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    def get_forecast(code):
        try:
            df = client.query_wind_and_solar_forecast(code, start=start, end=end)
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            df = df.T.groupby(level=0).sum().T.reset_index().rename(columns={'index': 'Time'})
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            return df
        except: return None
    all_forecast = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_forecast, code) for code in codes]
        for future in as_completed(futures):
            res_data = future.result(); 
            if res_data is not None: all_forecast.append(res_data)
    return pd.concat(all_forecast) if all_forecast else pd.DataFrame()

st.title("⚡ European Electricity Market Explorer")
all_zones = list(ZONE_NAMES.keys())
selected_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
plot_df = pd.DataFrame(); full_price_df = pd.DataFrame(); gen_df = pd.DataFrame(); forecast_df = pd.DataFrame() 

if len(d_range) == 2:
    if res in ["Daily", "Monthly"]:
        csv_raw = load_local_csv()
        if not csv_raw.empty:
            mask = (csv_raw['Date'] >= d_range[0]) & (csv_raw['Date'] <= d_range[1])
            data_subset = csv_raw[mask]
            
            # 1. Prices
            full_price_df = data_subset[data_subset['Metric'] == 'Baseload'].copy()
            full_price_df = full_price_df.rename(columns={'Date': 'Time', 'Country': 'Zone'})
            full_price_df['Time'] = pd.to_datetime(full_price_df['Time']).dt.tz_localize('Europe/Brussels')
            
            # 2. Actuals (for Capture Metrics)
            gen_subset = data_subset[data_subset['Metric'].str.contains('Generation')].copy()
            if not gen_subset.empty:
                gen_subset['Metric'] = gen_subset['Metric'].str.replace(' Generation', '')
                gen_pivot = gen_subset.pivot_table(index=['Date', 'Country'], columns='Metric', values='Price').reset_index()
                gen_pivot = gen_pivot.rename(columns={'Date': 'Time', 'Country': 'Zone'})
                gen_pivot['Time'] = pd.to_datetime(gen_pivot['Time']).dt.tz_localize('Europe/Brussels')
                gen_df = gen_pivot[gen_pivot['Zone'].isin(selected_codes)]
            
            # 3. Forecasts (Requirement 1)
            fore_subset = data_subset[data_subset['Metric'].str.contains('Forecast')].copy()
            if not fore_subset.empty:
                fore_subset['Metric'] = fore_subset['Metric'].str.replace(' Forecast', '')
                fore_pivot = fore_subset.pivot_table(index=['Date', 'Country'], columns='Metric', values='Price').reset_index()
                fore_pivot = fore_pivot.rename(columns={'Date': 'Time', 'Country': 'Zone'})
                fore_pivot['Time'] = pd.to_datetime(fore_pivot['Time']).dt.tz_localize('Europe/Brussels')
                forecast_df_raw = fore_pivot[fore_pivot['Zone'].isin(selected_codes)]
            else:
                forecast_df_raw = pd.DataFrame()
        else:
            st.error("Historical CSV not found. Please run data extraction.")
    else:
        full_price_df = fetch_data(all_zones, d_range[0], d_range[1])
        gen_df = fetch_gen_data(selected_codes, d_range[0], d_range[1])
        forecast_df_raw = fetch_forecast_data(selected_codes, d_range[0], d_range[1]) if selected_gen_types else pd.DataFrame()

    if not full_price_df.empty:
        res_map = {"15 min": "15min", "60 min": "60min", "Daily": "D", "Monthly": "MS"}
        freq = res_map.get(res, "60min")
        # FIXED: Changed group_keys=False to group_keys=True
        full_price_resampled = full_price_df.groupby('Zone', group_keys=True).apply(lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()).reset_index()
        plot_df = full_price_resampled[full_price_resampled['Zone'].isin(selected_codes)].copy()
        plot_df['Currency'] = plot_df['Zone'].apply(lambda x: ZONE_NAMES.get(x, ['', 'EUR'])[1])
        plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES.get(x, ['', 'EUR'])[1]}/MWh)")

        if not forecast_df_raw.empty:
            # FIXED: Changed group_keys=False to group_keys=True
            forecast_df = forecast_df_raw.groupby('Zone', group_keys=True).apply(lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()).reset_index()

# [Plotting and Map logic remains unchanged from original app.py]
col_chart, col_map = st.columns([2, 1])
with col_chart:
    st.subheader("Day-Ahead Prices and Generation Forecasts")
    if not plot_df.empty: 
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        colors = px.colors.qualitative.Plotly
        zone_color_map = {zone: colors[i % len(colors)] for i, zone in enumerate(selected_codes)}
        for zone in selected_codes:
            zone_df = plot_df[plot_df['Zone'] == zone]
            fig.add_trace(go.Scatter(x=zone_df['Time'], y=zone_df['Price'], name=f"{zone} Price ({ZONE_NAMES[zone][1]}/MWh)", line=dict(color=zone_color_map[zone], width=2)), secondary_y=False)
        if ppa_price > 0:
            fig.add_trace(go.Scatter(x=plot_df['Time'].unique(), y=[ppa_price]*len(plot_df['Time'].unique()), name="PPA Price", line=dict(color='red', dash='dash', width=2)), secondary_y=False)
        if selected_gen_types and not forecast_df.empty:
            for zone in selected_codes:
                z_gen_df = forecast_df[forecast_df['Zone'] == zone]
                for g_type in selected_gen_types:
                    if g_type in z_gen_df.columns:
                        fig.add_trace(go.Scatter(x=z_gen_df['Time'], y=z_gen_df[g_type], name=f"{zone} {g_type} Forecast (MW)", line=dict(color=zone_color_map[zone], dash='dot', width=1)), secondary_y=True)
        fig.update_layout(template="plotly_white", hovermode="x unified", legend=dict(orientation="h", y=-0.2), margin=dict(l=0, r=0, b=0, t=20))
        st.plotly_chart(fig, use_container_width=True)

with col_map:
    # [Map logic block - unchanged]
    geojson_folder = "geojson_files"
    if os.path.exists(geojson_folder):
        # ... (Map code from user's snippet)
        pass

st.divider()
col_met, col_tab = st.columns([1, 2])

with col_met:
    st.subheader("Key Metrics")
    if not plot_df.empty:
        key_metrics_list = []
        for code in selected_codes:
            z_df = plot_df[plot_df['Zone'] == code].set_index('Time')
            min_price = z_df['Price'].min()
            key_metrics_list.append({"Zone": code, "Number of Negative Periods": len(z_df[z_df['Price'] < 0]), "Lowest Price": f"{min_price:.2f} {ZONE_NAMES[code][1]}/MWh", "Lowest Price Date & Time": z_df['Price'].idxmin().strftime("%d-%m-%y %H:%M")})
        st.table(pd.DataFrame(key_metrics_list))

    st.subheader("Baseload & Capture Metrics")
    if not plot_df.empty and not gen_df.empty: # Requirement 2 Fix: gen_df is now populated in CSV branch
        metrics_list = []
        for code in selected_codes:
            p_sub = plot_df[plot_df['Zone'] == code].copy()
            g_sub = gen_df[gen_df['Zone'] == code].set_index('Time').resample(freq).sum(numeric_only=True).reset_index()
            if exclude_neg: p_sub['Price'] = p_sub['Price'].clip(lower=0)
            m_df = pd.merge(p_sub, g_sub, on='Time', how='inner')
            baseload = p_sub['Price'].mean()
            row = {"Zone": code, "Baseload": f"{baseload:.2f}", "Unit": f"{ZONE_NAMES[code][1]}/MWh"}
            for fuel in ['Solar', 'Wind Onshore', 'Wind Offshore']:
                cap = "N/A"
                if fuel in m_df.columns:
                    total = m_df[fuel].sum()
                    if total > 0: cap = f"{(m_df['Price'] * m_df[fuel]).sum() / total:.2f}"
                row[f"{fuel} Capture" if "Solar" in fuel else fuel] = cap
            metrics_list.append(row)
        st.table(pd.DataFrame(metrics_list))
    else:
        st.info("Select zones and ensure 'Actual Generation' is available to calculate Capture prices.")

with col_tab:
    # [Data Table logic - unchanged]
    st.subheader("Data Table")
    if not plot_df.empty:
        # ... (Table code from user's snippet)
        pass

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
#NEW ENTSOE-E MARKET FEED
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
    "LV": ["Update Latvia", "EUR"], "NO_1": ["Norway East", "EUR"], "NO_2": ["Norway South", "EUR"],
    "NO_3": ["Norway Central", "EUR"], "NO_4": ["Norway Northern", "EUR"], "NO_5": ["Norway West", "EUR"],
    "SE_1": ["Sweden Luleå", "EUR"], "SE_2": ["Sweden Sundsvall", "EUR"], "SE_3": ["Sweden Stockholm", "EUR"],
    "SE_4": ["Sweden Malmö", "EUR"], "ES": ["Spain", "EUR"], "PT": ["Portugal", "EUR"],
    # Balkan and Central/Eastern European Zones
    "HR": ["Croatia", "EUR"], "HU": ["Hungary", "EUR"], 
    "ME": ["Montenegro", "EUR",], "MK": ["North Macedonia", "EUR"],
    "RO": ["Romania", "EUR"], "RS": ["Serbia", "EUR"], 
    "SI": ["Slovenia", "EUR"], "SK": ["Slovakia", "EUR"],
    # Italian Zones
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
    
    # Use key="selected_zones" directly to sync with session state automatically
    st.multiselect("Select bidding zones:", 
                   options=sorted(display_options.keys()), 
                   key="selected_zones")
    
    gen_options = ["Solar", "Wind Onshore", "Wind Offshore"]
    selected_gen_types = st.multiselect("Overlay Generation Forecast:", options=gen_options, key="gen_forecast_select")
    res = st.radio("Resolution", ["Monthly", "Daily", "60 min", "15 min"], horizontal=True, key="res_radio")
    
    today = datetime.now().date()
    # Ensure d_range defaults to the session state if it exists to prevent reset on rerun
    default_d_range = st.session_state.get("date_range_input", (today - timedelta(days=2), today))
    d_range = st.date_input("Date Range", value=default_d_range, key="date_range_input")
    
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
        elif floor_rate_eur == 0 and floor_rate_pct == 0:
            st.warning("Please enter a floor rate for Market Following.")
            
    if (fixed_floating or market_following) and res != "Monthly":
        st.error("⚠️ Settlement not available on a Daily/60min/15min basis, please select Monthly.")
    
# --- LOAD CSV DATA ---
@st.cache_data
def load_local_csv():
    if os.path.exists('market_prices.csv'):
        df = pd.read_csv('market_prices.csv')
        # FIX: Added format='mixed' and dayfirst=True for robust date parsing
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, format='mixed').dt.date
        return df
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    
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
            res_data = future.result()
            if res_data is not None: all_data.append(res_data)
            
    return pd.concat(all_data) if all_data else pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_gen_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)

    def get_gen(code):
        try:
            df = client.query_generation(code, start=start, end=end)
            if isinstance(df, pd.Series): df = df.to_frame()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.T.groupby(level=0).sum().T 
            df.index.name = 'Time'
            df = df.reset_index()
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            return df
        except: return None

    all_gen = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_gen, code) for code in codes]
        for future in as_completed(futures):
            res_data = future.result()
            if res_data is not None: all_gen.append(res_data)
            
    return pd.concat(all_gen) if all_gen else pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_forecast_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    
    def get_forecast(code):
        try:
            df = client.query_wind_and_solar_forecast(code, start=start, end=end)
            if isinstance(df, pd.Series): df = df.to_frame()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.T.groupby(level=0).sum().T 
            df.index.name = 'Time'
            df = df.reset_index()
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            return df
        except: return None

    all_forecast = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_forecast, code) for code in codes]
        for future in as_completed(futures):
            res_data = future.result()
            if res_data is not None: all_forecast.append(res_data)
            
    return pd.concat(all_forecast) if all_forecast else pd.DataFrame()

st.title("⚡ European Electricity Market Explorer")
all_zones = list(ZONE_NAMES.keys())
selected_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
plot_df = pd.DataFrame()
full_price_df = pd.DataFrame()
gen_df = pd.DataFrame()
forecast_df = pd.DataFrame() 
forecast_df_raw = pd.DataFrame()

if len(d_range) == 2:
    if res in ["Daily", "Monthly"]:
        csv_raw = load_local_csv()
        if not csv_raw.empty:
            val_col = 'Price' if 'Price' in csv_raw.columns else ('Value' if 'Value' in csv_raw.columns else 'MW')
            mask = (csv_raw['Date'] >= d_range[0]) & (csv_raw['Date'] <= d_range[1])
            data_subset = csv_raw[mask].copy()
            
            full_price_df = data_subset[data_subset['Metric'] == 'Baseload'].copy()
            if not full_price_df.empty:
                full_price_df = full_price_df.rename(columns={'Date': 'Time', 'Country': 'Zone', val_col: 'Price'})
                full_price_df['Time'] = pd.to_datetime(full_price_df['Time']).dt.tz_localize('Europe/Brussels')
            
            gen_subset = data_subset[data_subset['Metric'].str.contains(' Generation', na=False)].copy()
            if not gen_subset.empty:
                gen_subset['Metric'] = gen_subset['Metric'].str.replace(' Generation', '', regex=False).str.strip()
                gen_pivot = gen_subset.pivot_table(index=['Date', 'Country'], columns='Metric', values=val_col).reset_index()
                gen_pivot = gen_pivot.rename(columns={'Date': 'Time', 'Country': 'Zone'})
                gen_pivot['Time'] = pd.to_datetime(gen_pivot['Time']).dt.tz_localize('Europe/Brussels')
                gen_df = gen_pivot[gen_pivot['Zone'].isin(selected_codes)]
                
            forecast_subset = data_subset[data_subset['Metric'].str.contains(' Forecast', na=False)].copy()
            if not forecast_subset.empty:
                forecast_subset['Metric'] = forecast_subset['Metric'].str.replace(' Forecast', '', regex=False).str.strip()
                fc_pivot = forecast_subset.pivot_table(index=['Date', 'Country'], columns='Metric', values=val_col).reset_index()
                fc_pivot = fc_pivot.rename(columns={'Date': 'Time', 'Country': 'Zone'})
                fc_pivot['Time'] = pd.to_datetime(fc_pivot['Time']).dt.tz_localize('Europe/Brussels')
                forecast_df_raw = fc_pivot[fc_pivot['Zone'].isin(selected_codes)]
    else:
        full_price_df = fetch_data(all_zones, d_range[0], d_range[1])
        gen_df = fetch_gen_data(selected_codes, d_range[0], d_range[1])
        if selected_gen_types:
            forecast_df_raw = fetch_forecast_data(selected_codes, d_range[0], d_range[1])

    if not full_price_df.empty:
        res_map = {"15 min": "15min", "60 min": "60min", "Daily": "D", "Monthly": "MS"}
        freq = res_map.get(res, "60min")
        
        full_price_resampled = full_price_df.groupby('Zone').apply(
            lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()
        ).reset_index()
        
        plot_df = full_price_resampled[full_price_resampled['Zone'].isin(selected_codes)].copy()
        
        if not forecast_df_raw.empty:
            forecast_df = forecast_df_raw.groupby('Zone').apply(
                lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()
            ).reset_index()

col_chart, col_map = st.columns([2, 1])
with col_chart:
    st.subheader("Day-Ahead Prices and Generation Forecasts")
    if not plot_df.empty: 
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        colors = px.colors.qualitative.Plotly
        zone_color_map = {zone: colors[i % len(colors)] for i, zone in enumerate(selected_codes)}
        
        for zone in selected_codes:
            zone_df = plot_df[plot_df['Zone'] == zone]
            currency = ZONE_NAMES[zone][1]
            fig.add_trace(go.Scatter(x=zone_df['Time'], y=zone_df['Price'], name=f"{zone} Price ({currency}/MWh)", line=dict(color=zone_color_map[zone], width=2), hovertemplate="%{fullData.name}: %{y:.2f}<extra></extra>"), secondary_y=False)
        
        if ppa_price > 0:
            fig.add_trace(go.Scatter(x=plot_df['Time'].unique(), y=[ppa_price]*len(plot_df['Time'].unique()), name="PPA Price", line=dict(color='red', dash='dash', width=2), hovertemplate="PPA Price (EUR/MWh): %{y:.2f}<extra></extra>"), secondary_y=False)
        
        if selected_gen_types and not forecast_df.empty:
            for zone in selected_codes:
                z_gen_df = forecast_df[forecast_df['Zone'] == zone]
                if not z_gen_df.empty:
                    for g_type in selected_gen_types:
                        if g_type in z_gen_df.columns:
                            fig.add_trace(go.Scatter(x=z_gen_df['Time'], y=z_gen_df[g_type], name=f"{zone} {g_type} Forecast (MW)", line=dict(color=zone_color_map[zone], dash='dot', width=1), hovertemplate="%{fullData.name}: %{y:.2f}<extra></extra>"), secondary_y=True)

        fig.update_layout(template="plotly_white", hovermode="x unified", legend=dict(orientation="h", y=-0.2), margin=dict(l=0, r=0, b=0, t=20))
        st.plotly_chart(fig, use_container_width=True)

with col_map:
    def load_and_get_centers(folder_path):
        combined, centers, found_zones = {"type": "FeatureCollection", "features": []}, [], []
        files = glob.glob(os.path.join(folder_path, "*.geojson")) + glob.glob(os.path.join(folder_path, "*.txt"))
        for file in files:
            try:
                with open(file, "r") as f:
                    data = json.load(f)
                    features = data["features"] if "features" in data else [data]
                    for feature in features:
                        combined["features"].append(feature)
                        z_name = feature["properties"]["zoneName"]
                        found_zones.append(z_name)
                        geom = feature["geometry"]
                        coords = np.array(geom["coordinates"][0]) if geom["type"] == "Polygon" else np.array(max(geom["coordinates"], key=lambda x: len(x[0]))[0])
                        min_lon, min_lat = np.min(coords, axis=0)
                        max_lon, max_lat = np.max(coords, axis=0)
                        centers.append({"Zone": z_name, "lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2})
            except: continue
        return combined, pd.DataFrame(centers), found_zones

    geojson_folder = "geojson_files"
    if os.path.exists(geojson_folder):
        geojson_data, centers_df, all_found_codes = load_and_get_centers(geojson_folder)
        if geojson_data["features"]:
            avg_prices = full_price_resampled.groupby('Zone')['Price'].mean().to_dict() if not full_price_resampled.empty else {}
            map_df = pd.DataFrame([{"Zone": k, "Selected": 1 if k in selected_codes else 0, "AvgPrice": f"{avg_prices.get(k, 0):.2f}", "Currency": ZONE_NAMES.get(k, ["", "EUR"])[1]} for k in all_found_codes])
            fig_map = px.choropleth(map_df, geojson=geojson_data, locations="Zone", featureidkey="properties.zoneName", color="Selected", color_continuous_scale=["#262730", "#007927"], custom_data=["AvgPrice", "Currency"])
            fig_map.update_geos(center=dict(lon=12, lat=52), projection_scale=7, projection_type="mercator", bgcolor="rgba(0,0,0,0)")
            fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500, coloraxis_showscale=False, paper_bgcolor="rgba(0,0,0,0)")
            
            map_event = st.plotly_chart(fig_map, use_container_width=True, on_select="rerun", selection_mode="points")
            if map_event and "selection" in map_event and map_event["selection"]["points"]:
                clicked_code = map_event["selection"]["points"][0]["location"]
                clicked_label = f"{ZONE_NAMES[clicked_code][0]} ({clicked_code})"
                curr = list(st.session_state.selected_zones)
                if clicked_label in curr: curr.remove(clicked_label)
                else: curr.append(clicked_label)
                st.session_state.selected_zones = curr
                st.rerun()

st.divider()
col_met, col_tab = st.columns([1, 2])
with col_met:
    st.subheader("Key Metrics")
    if not plot_df.empty:
        metrics = []
        for code in selected_codes:
            z_df = plot_df[plot_df['Zone'] == code].set_index('Time')
            metrics.append({"Zone": code, "Negative Periods": len(z_df[z_df['Price'] < 0]), "Lowest Price": f"{z_df['Price'].min():.2f} {ZONE_NAMES[code][1]}/MWh"})
        st.table(pd.DataFrame(metrics))

with col_tab:
    st.subheader("Data Table")
    if not plot_df.empty:
        table_df = plot_df.copy()
        if fixed_floating and ppa_price > 0 and res == "Monthly":
            table_df['Fixed for Floating settlement'] = table_df['Price'] - ppa_price
        if market_following and ppa_price > 0 and res == "Monthly":
            eff_floor = floor_rate_eur if floor_rate_eur > 0 else (floor_rate_pct / 100 * ppa_price)
            table_df['Market following settlement'] = np.where(table_df['Price'] > ppa_price, eff_floor, table_df['Price'] - ppa_price)
        table_df['Date'] = table_df['Time'].dt.strftime('%d-%m-%Y')
        st.dataframe(table_df.drop(columns=['Time']).style.format(precision=2, na_rep="-"), use_container_width=True, height=400)

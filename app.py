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

# --- CSS FOR CUSTOM LAYOUT ---
st.markdown("""
    <style>
    /* Widened sidebar for longer zone names */
    section[data-testid="stSidebar"] { width: 400px !important; }
    .main .block-container { 
        padding-top: 2rem !important;
        max-width: 98% !important; 
    }
    </style>
    """, unsafe_allow_html=True)

if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

# --- SIDEBAR: CONTROLS ---
with st.sidebar:
    st.title("Configuration")
    
    # Bidding Zone Search in Sidebar
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
    return pd.concat(all_data) if all_data else pd.DataFrame()

# --- MAIN AREA ---
st.title("⚡ Energy Market Explorer")

# Pre-fetch data
codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
plot_df = pd.DataFrame()
if len(d_range) == 2 and codes:
    data = fetch_data(codes, d_range[0], d_range[1])
    if not data.empty:
        freq = '60min' if res == "60 min" else '15min'
        plot_df = data.groupby('Zone').apply(
            lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()
        ).reset_index()
        plot_df['Currency'] = plot_df['Zone'].apply(lambda x: ZONE_NAMES.get(x, ['', 'EUR'])[1])
        plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES.get(x, ['', 'EUR'])[1]}/MWh)")

# --- MIDDLE SECTION (CHART & MAP) ---
col_chart, col_map = st.columns([2, 1])

with col_chart:
    st.subheader("Day-Ahead Prices")
    if not plot_df.empty:
        fig_line = px.line(plot_df, x='Time', y='Price', color='Display', template="plotly_white", 
                           custom_data=['Currency'])
        fig_line.update_layout(
            legend=dict(orientation="h", y=-0.2), 
            margin=dict(l=0, r=0, b=0, t=20),
            hovermode="closest"
        )
        # Custom Hover Template
        fig_line.update_traces(
            hovertemplate="<b>Date:</b> %{x|%b %d %Y}<br>" +
                          "<b>Time:</b> %{x|%H:%M}<br>" +
                          "<b>Price:</b> %{y:.2f} %{customdata[0]}/MWh<extra></extra>"
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Select zones to view price trends.")

with col_map:
    def load_and_get_centers(folder_path):
        combined = {"type": "FeatureCollection", "features": []}
        centers = []
        found_zones = []
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
                        if geom["type"] == "Polygon":
                            coords = np.array(geom["coordinates"][0])
                        elif geom["type"] == "MultiPolygon":
                            coords = np.array(max(geom["coordinates"], key=lambda x: len(x[0]))[0])
                        if len(coords) > 0:
                            min_lon, min_lat = np.min(coords, axis=0)
                            max_lon, max_lat = np.max(coords, axis=0)
                            centers.append({"Zone": z_name, "lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2})
            except: continue
        return combined, pd.DataFrame(centers), found_zones

    geojson_folder = "geojson_files"
    if os.path.exists(geojson_folder):
        geojson_data, centers_df, all_found_codes = load_and_get_centers(geojson_folder)
        if geojson_data["features"]:
            current_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
            
            # Calculate Average Prices for selected date range per zone
            avg_prices = {}
            if not plot_df.empty:
                avg_prices = plot_df.groupby('Zone')['Price'].mean().to_dict()

            map_rows = []
            for k in all_found_codes:
                price = avg_prices.get(k, None)
                currency = ZONE_NAMES.get(k, ["", "EUR"])[1]
                map_rows.append({
                    "Zone": k, 
                    "Selected": 1 if k in current_codes else 0,
                    "AvgPrice": f"{price:.2f}" if price is not None else "N/A",
                    "Currency": currency
                })
            map_df = pd.DataFrame(map_rows)

            fig_map = px.choropleth(
                map_df, geojson=geojson_data, locations="Zone", 
                featureidkey="properties.zoneName", color="Selected",
                color_continuous_scale=["#262730", "#007927"],
                custom_data=["AvgPrice", "Currency"]
            )

            fig_map.update_traces(
                hovertemplate="<b>Zone:</b> %{location}<br>" +
                              "<b>Avg Price:</b> %{customdata[0]} %{customdata[1]}/MWh<extra></extra>"
            )

            if not centers_df.empty:
                fig_map.add_scattergeo(
                    lat=centers_df['lat'], lon=centers_df['lon'], text=centers_df['Zone'],
                    mode='text', textfont=dict(size=10, color="#FFFFFF", family="Arial Black"),
                    showlegend=False,
                    hoverinfo="skip" # Removes hover from the zone labels
                )

            fig_map.update_geos(
                center=dict(lon=12, lat=52), projection_scale=7, 
                visible=True, 
                showcountries=True, 
                countrycolor="#262730", 
                lakecolor="white",
                landcolor="#e0e0e0", 
                projection_type="mercator", 
                bgcolor="rgba(0,0,0,0)"
            )

            fig_map.update_layout(
                margin={"r":0,"t":0,"l":0,"b":0}, height=500, 
                coloraxis_showscale=False, paper_bgcolor="rgba(0,0,0,0)",
                autosize=True, modebar=dict(bgcolor='rgba(0,0,0,0)', color='gray', orientation='v')
            )
            st.plotly_chart(fig_map, use_container_width=True, config={'displaylogo': False})

# --- BOTTOM SECTION (DATA TABLE) ---
st.divider()
st.subheader("Price Data Explorer")
if not plot_df.empty:
    plot_df['Date'] = plot_df['Time'].dt.strftime('%d-%m-%Y')
    plot_df['24h Time'] = plot_df['Time'].dt.strftime('%H:%M')
    pivot = plot_df.pivot_table(index=['Date', '24h Time'], columns='Display', values='Price')
    st.dataframe(pivot.style.format("{:.2f}"), use_container_width=True, height=400)

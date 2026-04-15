import streamlit as st
import pandas as pd
import plotly.express as px
import os
import json
import requests
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# URL for a GeoJSON containing European Bidding Zones (highly recommended for SE, NO, IT borders)
GEOJSON_URL = "https://raw.githubusercontent.com/Applied-Energy-Solutions/european-bidding-zones-geojson/master/bidding_zones.geojson"

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
    "GR": ["Greece", "EUR"], "HR": ["Croatia", "EUR"], "HU": ["Hungary", "EUR"],
    "RO": ["Romania", "EUR"], "RS": ["Serbia", "EUR"], "SI": ["Slovenia", "EUR"],
    "SK": ["Slovakia", "EUR"], "IT_NORD": ["Italy North", "EUR"],
    "IT_CNOR": ["Italy C-North", "EUR"], "IT_CSUD": ["Italy C-South", "EUR"],
    "IT_SUD": ["Italy South", "EUR"], "IT_SICI": ["Sicily", "EUR"],
    "IT_SARD": ["Sardinia", "EUR"], "IT_CALA": ["Calabria", "EUR"]
}

st.set_page_config(page_title="Energy Market Map", layout="wide", initial_sidebar_state="collapsed")

# 2. Session State
if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

# 3. Custom CSS for Transparency & Full Width
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    [data-testid="stSidebar"] { min-width: 500px !important; max-width: 600px !important; }
    </style>
    """, unsafe_allow_html=True)

# 4. Fetching Logic
@st.cache_data(ttl=3600)
def fetch_live_data(selected_codes, start_date, end_date):
    if not selected_codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    all_data = []
    for code in selected_codes:
        try:
            series = client.query_day_ahead_prices(code, start=start, end=end)
            df_t = series.to_frame(name='Price').reset_index()
            df_t.columns = ['Time', 'Price']
            df_t['Zone'] = code
            df_t['Currency'] = ZONE_NAMES[code][1]
            df_t['Time'] = pd.to_datetime(df_t['Time'], utc=True)
            all_data.append(df_t)
        except: pass
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# 5. Main Map View
st.title("⚡ European Bidding Zone Explorer")

# Interactive Search (Acting as map controller)
display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}
selected_labels = st.multiselect(
    "Search and select bidding zones to open the data panel:",
    options=sorted(display_options.keys()),
    key="selected_zones"
)

# Render Map
map_data = []
current_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
for code, info in ZONE_NAMES.items():
    map_data.append({"Zone": code, "Name": info[0], "Selected": 1 if code in current_codes else 0})

fig_map = px.choropleth(
    pd.DataFrame(map_data),
    locations="Zone",
    color="Selected",
    hover_name="Name",
    locationmode='ISO-3', # Note: For true sub-borders, you'd link the GeoJSON here
    color_continuous_scale=["#262730", "#1f77b4"],
    scope="europe"
)

fig_map.update_layout(
    coloraxis_showscale=False,
    margin={"r":0,"t":0,"l":0,"b":0},
    height=700,
    paper_bgcolor='rgba(0,0,0,0)', # Transparent background
    plot_bgcolor='rgba(0,0,0,0)',
    geo=dict(bgcolor='rgba(0,0,0,0)', lakecolor='rgba(0,0,0,0)')
)
st.plotly_chart(fig_map, use_container_width=True)

# 6. The "Side Window" (Standard Sidebar used as a Result Panel)
if st.session_state.selected_zones:
    with st.sidebar:
        st.header("📊 Market Data Panel")
        st.write("Results for: " + ", ".join([lbl.split(" (")[0] for lbl in st.session_state.selected_zones]))
        
        # Settings inside the panel
        res = st.selectbox("Resolution", ["60 min", "15 min"])
        date_range = st.date_input("Range", value=(datetime.now().date() - timedelta(days=2), datetime.now().date()))
        
        if len(date_range) == 2:
            codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
            raw_df = fetch_live_data(codes, date_range[0], date_range[1])
            
            if not raw_df.empty:
                raw_df['Time'] = raw_df['Time'].dt.tz_convert('Europe/Brussels')
                plot_df = raw_df.groupby('Zone').resample('60min' if res=="60 min" else '15min', on='Time')['Price'].mean().reset_index()
                plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES[x][1]}/MWh)")
                
                # Big Chart
                fig_line = px.line(plot_df, x='Time', y='Price', color='Display', template="plotly_dark")
                fig_line.update_layout(legend=dict(orientation="h", y=-0.2))
                st.plotly_chart(fig_line, use_container_width=True)
                
                # Table
                st.subheader("Raw Data")
                st.dataframe(plot_df.pivot(index='Time', columns='Display', values='Price').tail(50))
            else:
                st.info("Fetching data...")
else:
    st.sidebar.write("Select a bidding zone on the main page to view price analytics.")

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

# --- CSS FOR SIDEBAR & TRANSPARENCY ---
st.markdown("""
    <style>
    section[data-testid="stSidebar"] { width: 600px !important; }
    .block-container { padding-top: 1rem; }
    /* Making the main background blend with the map if needed */
    .stApp { background-color: transparent; }
    </style>
    """, unsafe_allow_html=True)

if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

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
            df['Zone'] = code
            df['Currency'] = ZONE_NAMES[code][1]
            all_data.append(df)
        except: continue
    return pd.concat(all_data) if all_data else pd.DataFrame()

# --- SIDEBAR: MARKET ANALYTICS ---
with st.sidebar:
    st.title("📊 Market Analytics")
    if st.session_state.selected_zones:
        res = st.radio("Resolution", ["60 min", "15 min"], horizontal=True)
        today = datetime.now().date()
        d_range = st.date_input("Date Range", value=(today - timedelta(days=2), today))
        
        display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}
        codes = [display_options[lbl] for lbl in st.session_state.selected_zones]

        if len(d_range) == 2:
            with st.spinner("Updating Analytics..."):
                data = fetch_data(codes, d_range[0], d_range[1])
            if not data.empty:
                data['Time'] = pd.to_datetime(data['Time']).dt.tz_convert('Europe/Brussels')
                
                # Fixed Pivot Logic to avoid the 2-d error
                plot_df = data.copy()
                plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES[x][1]}/MWh)")
                
                fig_line = px.line(plot_df, x='Time', y='Price', color='Display', template="plotly_white")
                fig_line.update_layout(legend=dict(orientation="h", y=-0.3), margin=dict(l=0, r=0, b=0, t=20), hovermode="x unified")
                st.plotly_chart(fig_line, use_container_width=True)
                
                st.subheader("Data Table")
                plot_df['Date'] = plot_df['Time'].dt.strftime('%d-%m-%Y')
                plot_df['24h Time'] = plot_df['Time'].dt.strftime('%H:%M')
                
                # Pivot table fix: ensured unique index
                pivot = plot_df.pivot_table(index=['Date', '24h Time'], columns='Display', values='Price', aggfunc='mean')
                st.dataframe(pivot.style.format("{:.2f}"), use_container_width=True)
    else:
        st.info("Select a bidding zone on the map to see data here.")

# --- MAIN PAGE: SEARCH & MAP ---
st.subheader("Search and select bidding zones")
display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}
st.multiselect("Select zones:", options=sorted(display_options.keys()), key="selected_zones", label_visibility="collapsed")

# --- LOGIC TO LOAD AND COMBINE INDIVIDUAL GEOJSON FILES ---
def load_and_get_centers(folder_path):
    combined = {"type": "FeatureCollection", "features": []}
    centers = []
    files = glob.glob(os.path.join(folder_path, "*.geojson"))
    
    for file in files:
        try:
            with open(file, "r") as f:
                data = json.load(f)
                feature = data["features"][0] if "features" in data else data
                combined["features"].append(feature)
                
                # Simple centroid calculation for labels
                geom = feature["geometry"]
                coords = []
                if geom["type"] == "Polygon":
                    coords = np.array(geom["coordinates"][0])
                elif geom["type"] == "MultiPolygon":
                    # Use the first polygon of the multipolygon for the label
                    coords = np.array(geom["coordinates"][0][0])
                
                if len(coords) > 0:
                    lon, lat = np.mean(coords, axis=0)
                    centers.append({"Zone": feature["properties"]["zoneName"], "lat": lat, "lon": lon})
        except: continue
    return combined, pd.DataFrame(centers)

geojson_folder = "geojson_files"

if os.path.exists(geojson_folder):
    geojson_data, centers_df = load_and_get_centers(geojson_folder)
    
    if geojson_data["features"]:
        current_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
        map_df = pd.DataFrame([{"Zone": k, "Selected": 1 if k in current_codes else 0} for k in ZONE_NAMES.keys()])

        # Create the Base Choropleth
        fig_map = px.choropleth(
            map_df, 
            geojson=geojson_data,
            locations="Zone", 
            featureidkey="properties.zoneName",
            color="Selected",
            color_continuous_scale=["#f2f2f2", "#1f77b4"],
            scope="europe"
        )

        # Add the Text Overlay (Zone Codes)
        if not centers_df.empty:
            fig_map.add_scattergeo(
                lat=centers_df['lat'],
                lon=centers_df['lon'],
                text=centers_df['Zone'],
                mode='text',
                textfont=dict(size=10, color="black"),
                showlegend=False
            )

        # Map Layout Adjustments (Size, Transparency, Centering)
        fig_map.update_geos(
            fitbounds="locations",
            visible=False,
            bgcolor='rgba(0,0,0,0)',  # Transparent map background
            projection_type="mercator"
        )

        fig_map.update_layout(
            margin={"r":0,"t":0,"l":0,"b":0},
            height=1000, # Increased height to take up main section
            coloraxis_showscale=False,
            paper_bgcolor='rgba(0,0,0,0)', # Transparent paper
            plot_bgcolor='rgba(0,0,0,0)'   # Transparent plot
        )

        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("No shapes found in the geojson_files folder.")
else:
    st.warning(f"Folder '{geojson_folder}' not found.")

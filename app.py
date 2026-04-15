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

# --- EXACT BRAND COLORS ---
B_LIGHT_BLUE = "#A0E7EE"
B_LIME = "#CDFC57"
B_ORANGE = "#FFB04C"
B_NAVY = "#275B7F"
B_GREEN = "#007927"
B_GREY = "#616469"
B_DARK_BG = "#111827" # Professional deep slate for background

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

# --- BRANDED CSS ---
st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

    /* Typography */
    html, body, [class*="css"] {{
        font-family: 'Inter', 'Arial', sans-serif !important;
    }}

    /* Sidebar Width and Color */
    section[data-testid="stSidebar"] {{
        width: 350px !important;
        background-color: {B_GREY} !important;
    }}
    
    section[data-testid="stSidebar"] * {{
        color: white !important;
    }}

    /* Main App Background */
    .stApp {{
        background-color: {B_DARK_BG};
    }}

    /* Header Styling */
    h1, h2, h3 {{
        color: {B_LIGHT_BLUE} !important;
        font-weight: 700 !important;
    }}

    /* Table & Dataframe Styling */
    [data-testid="stDataFrame"] {{
        border: 1px solid {B_NAVY} !important;
    }}

    .main .block-container {{ 
        padding-top: 2rem !important;
        max-width: 98% !important; 
    }}
    </style>
    """, unsafe_allow_html=True)

if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

# --- SIDEBAR ---
with st.sidebar:
    st.title("⚙️ Controls")
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

codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
plot_df = pd.DataFrame()
if len(d_range) == 2 and codes:
    data = fetch_data(codes, d_range[0], d_range[1])
    if not data.empty:
        freq = '60min' if res == "60 min" else '15min'
        plot_df = data.groupby('Zone').apply(
            lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()
        ).reset_index()
        plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES.get(x, ['', 'EUR'])[1]}/MWh)")

# --- VISUALS ---
col_chart, col_map = st.columns([2, 1])

with col_chart:
    st.subheader("Day-Ahead Prices")
    if not plot_df.empty:
        fig_line = px.line(
            plot_df, x='Time', y='Price', color='Display', 
            color_discrete_sequence=[B_ORANGE, B_LIME, B_LIGHT_BLUE, B_GREEN]
        )
        fig_line.update_layout(
            font_family="Inter",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=-0.2, font=dict(color="white")), 
            margin=dict(l=0, r=0, b=0, t=20),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color=B_LIGHT_BLUE)),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color=B_LIGHT_BLUE)),
            hovermode="x unified"
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
                        coords = []
                        def flatten(l):
                            for item in l:
                                if isinstance(item, list) and len(item) > 0 and isinstance(item[0], (list, float, int)):
                                    if isinstance(item[0], (float, int)): coords.append(item)
                                    else: flatten(item)
                        flatten(feature["geometry"]["coordinates"])
                        if coords:
                            np_coords = np.array(coords)
                            centers.append({"Zone": z_name, "lat": np.mean(np_coords[:, 1]), "lon": np.mean(np_coords[:, 0])})
            except: continue
        return combined, pd.DataFrame(centers), found_zones

    geojson_folder = "geojson_files"
    if os.path.exists(geojson_folder):
        geojson_data, centers_df, all_found_codes = load_and_get_centers(geojson_folder)
        if geojson_data["features"]:
            current_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
            map_df = pd.DataFrame([{"Zone": k, "Selected": 1 if k in current_codes else 0} for k in all_found_codes])

            fig_map = px.choropleth(
                map_df, geojson=geojson_data, locations="Zone", 
                featureidkey="properties.zoneName", color="Selected",
                color_continuous_scale=[B_GREY, B_NAVY] # Unselected is Grey, Selected is Navy
            )
            if not centers_df.empty:
                fig_map.add_scattergeo(
                    lat=centers_df['lat'], lon=centers_df['lon'], text=centers_df['Zone'],
                    mode='text', textfont=dict(size=9, color="white", family="Arial Bold"),
                    showlegend=False
                )
            fig_map.update_geos(
                fitbounds="locations", visible=True, showcountries=True, 
                countrycolor="rgba(255,255,255,0.1)", bgcolor="rgba(0,0,0,0)"
            )
            fig_map.update_layout(
                margin={"r":0,"t":0,"l":0,"b":0}, height=500, 
                coloraxis_showscale=False, paper_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig_map, use_container_width=True, config={'displaylogo': False})

# --- DATA TABLE ---
st.divider()
st.subheader("Price Data Explorer")
if not plot_df.empty:
    plot_df['Date'] = plot_df['Time'].dt.strftime('%d-%m-%Y')
    plot_df['24h Time'] = plot_df['Time'].dt.strftime('%H:%M')
    pivot = plot_df.pivot_table(index=['Date', '24h Time'], columns='Display', values='Price')
    st.dataframe(
        pivot.style.format("{:.2f}").set_table_styles([
            {'selector': 'th', 'props': [('background-color', B_NAVY), ('color', 'white')]}
        ]), 
        use_container_width=True, height=400
    )

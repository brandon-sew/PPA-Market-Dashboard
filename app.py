import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Config & API Setup
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Mapping - Keys match the GeoJSON names for highlighting
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

# Set sidebar to expanded by default to act as your "Side Window"
st.set_page_config(page_title="Market Explorer", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM CSS FOR SIDEBAR WIDTH ---
st.markdown("""
    <style>
    /* Adjust Sidebar width to be large enough for charts */
    section[data-testid="stSidebar"] {
        width: 600px !important; 
    }
    .block-container { padding-top: 2rem; }
    </style>
    """, unsafe_allow_html=True)

# 2. Session State Initialization
if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

# 3. Data Fetching
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

# --- SIDEBAR: MARKET ANALYTICS SECTION ---
with st.sidebar:
    st.title("📊 Market Analytics")
    
    if st.session_state.selected_zones:
        # Panel Controls
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
                res_val = '60min' if res=="60 min" else '15min'
                plot_df = data.groupby('Zone').resample(res_val, on='Time')['Price'].mean().reset_index()
                
                plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES[x][1]}/MWh)")
                plot_df['Date'] = plot_df['Time'].dt.strftime('%d-%m-%Y')
                plot_df['24h Time'] = plot_df['Time'].dt.strftime('%H:%M')

                # Chart
                has_non_eur = any(ZONE_NAMES[c][1] != 'EUR' for c in codes)
                y_title = "Price" if has_non_eur else "Price (EUR/MWh)"
                
                fig_line = px.line(plot_df, x='Time', y='Price', color='Display', labels={'Price': y_title}, template="plotly_white")
                fig_line.update_layout(legend=dict(orientation="h", y=-0.3), margin=dict(l=0, r=0, b=0, t=20), hovermode="x unified")
                st.plotly_chart(fig_line, use_container_width=True)
                
                # Data Table
                st.subheader("Data Table")
                pivot = plot_df.pivot_table(index=['Date', '24h Time'], columns='Display', values='Price')
                st.dataframe(pivot.style.format("{:.2f}"), use_container_width=True)
    else:
        st.info("Select a bidding zone on the map to see data here.")

# --- MAIN PAGE: MAP SECTION ---
st.subheader("Search and select bidding zones")
display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}

st.multiselect(
    "Select zones:", options=sorted(display_options.keys()), 
    key="selected_zones", label_visibility="collapsed"
)

# Map Logic
current_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
map_df = pd.DataFrame([{"Zone": k, "Selected": 1 if k in current_codes else 0} for k in ZONE_NAMES.keys()])

fig_map = px.choropleth(
    map_df, 
    geojson="https://raw.githubusercontent.com/Applied-Energy-Solutions/european-bidding-zones-geojson/master/bidding_zones.geojson",
    locations="Zone", featureidkey="properties.name",
    color="Selected",
    color_continuous_scale=["#f2f2f2", "#1f77b4"],
    scope="europe"
)

fig_map.update_layout(
    margin={"r":0,"t":0,"l":0,"b":0},
    height=800, coloraxis_showscale=False,
    geo=dict(showframe=False, showcoastlines=True, projection_type='mercator'),
    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
)

st.plotly_chart(fig_map, use_container_width=True)

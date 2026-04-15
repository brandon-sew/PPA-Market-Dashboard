import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Expanded Mapping - Keys MUST match the "name" property in the GeoJSON for highlighting to work
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
    "SE_4": ["Sweden (South)", "EUR"], "ES": ["Spain", "EUR"], "PT": ["Portugal", "EUR"],
    "GR": ["Greece", "EUR"], "HR": ["Croatia", "EUR"], "HU": ["Hungary", "EUR"],
    "RO": ["Romania", "EUR"], "RS": ["Serbia", "EUR"], "SI": ["Slovenia", "EUR"],
    "SK": ["Slovakia", "EUR"], "IT_NORD": ["Italy North", "EUR"],
    "IT_CNOR": ["Italy C-North", "EUR"], "IT_CSUD": ["Italy C-South", "EUR"],
    "IT_SUD": ["Italy South", "EUR"], "IT_SICI": ["Sicily", "EUR"],
    "IT_SARD": ["Sardinia", "EUR"], "IT_CALA": ["Calabria", "EUR"]
}

st.set_page_config(page_title="Energy Market Explorer", layout="wide", initial_sidebar_state="expanded")

# --- CSS FOR FULL WIDTH & TRANSPARENCY ---
st.markdown("""
    <style>
    /* Make the main container full width */
    .block-container { padding-top: 1rem; padding-bottom: 0rem; padding-left: 1rem; padding-right: 1rem; }
    /* Hide the streamlit header/footer for more space */
    header {visibility: hidden;}
    footer {visibility: hidden;}
    /* Custom Sidebar Width */
    [data-testid="stSidebar"] { min-width: 35%; max-width: 45%; }
    </style>
    """, unsafe_allow_html=True)

# 2. Session State for Selection
if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)", "Great Britain (GB)"]

# 3. Data Fetching
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
        except: st.sidebar.warning(f"Data for {code} is temporarily unavailable.")
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# 4. MAIN INTERFACE (MAP)
display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}

selected_labels = st.multiselect(
    "Search and select bidding zones",
    options=sorted(display_options.keys()),
    key="selected_zones",
)

# Building the Map Data
current_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
map_rows = []
for code, info in ZONE_NAMES.items():
    map_rows.append({"Zone": code, "Name": info[0], "Selected": 1 if code in current_codes else 0})
df_map = pd.DataFrame(map_rows)

# Using Mapbox for internal borders (Norway, Sweden, Italy)
fig_map = px.choropleth_mapbox(
    df_map, 
    geojson="https://raw.githubusercontent.com/Applied-Energy-Solutions/european-bidding-zones-geojson/master/bidding_zones.geojson",
    locations="Zone", 
    featureidkey="properties.name", # This links our code to the GeoJSON boundary
    color="Selected",
    hover_name="Name",
    mapbox_style="carto-positron",
    center={"lat": 52, "lon": 10},
    zoom=3,
    color_continuous_scale=["#e0e0e0", "#1f77b4"], # Gray for unselected, Blue for selected
    opacity=0.6
)

fig_map.update_layout(
    margin={"r":0,"t":0,"l":0,"b":0},
    height=850, # Full page height
    coloraxis_showscale=False,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

st.plotly_chart(fig_map, use_container_width=True)

# 5. SIDEBAR (CHART & TABLE)
with st.sidebar:
    if st.session_state.selected_zones:
        st.title("📊 Market Analytics")
        
        # UI Settings
        res = st.radio("Resolution", ["60 min", "15 min"], horizontal=True)
        today = datetime.now().date()
        date_range = st.date_input("Range", value=(today - timedelta(days=2), today))

        if len(date_range) == 2:
            codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
            with st.spinner("Fetching data..."):
                raw_df = fetch_live_data(codes, date_range[0], date_range[1])

            if not raw_df.empty:
                raw_df['Time'] = raw_df['Time'].dt.tz_convert('Europe/Brussels')
                
                # Resampling and Formatting
                plot_df = (
                    raw_df.groupby('Zone')
                    .resample('60min' if res=="60 min" else '15min', on='Time')['Price']
                    .mean().reset_index()
                )
                plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES[x][1]}/MWh)")
                plot_df['Date'] = plot_df['Time'].dt.strftime('%d-%m-%Y')
                plot_df['24h Time'] = plot_df['Time'].dt.strftime('%H:%M')

                # Determine Y-Axis Title
                has_non_eur = any(ZONE_NAMES[c][1] != 'EUR' for c in codes)
                y_title = "Price" if has_non_eur else "Price (EUR/MWh)"

                # Chart
                fig_line = px.line(
                    plot_df, x='Time', y='Price', color='Display',
                    labels={'Price': y_title}, template="plotly_white"
                )
                fig_line.update_layout(legend=dict(orientation="h", y=-0.3), hovermode="x unified")
                st.plotly_chart(fig_line, use_container_width=True)

                # Data Table (Restored to previous format)
                st.subheader("Data Table")
                pivot_df = plot_df.pivot_table(index=['Date', '24h Time'], columns='Display', values='Price')
                st.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)
    else:
        st.title("⚡ Welcome")
        st.info("Select one or more bidding zones on the main screen to begin analysis.")

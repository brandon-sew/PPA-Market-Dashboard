import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

ZONE_NAMES = {
    "AT": ["Austria", "EUR", "AUT"], "BE": ["Belgium", "EUR", "BEL"], "BG": ["Bulgaria", "EUR", "BGR"],
    "CH": ["Switzerland", "EUR", "CHE"], "CZ": ["Czech Republic", "EUR", "CZE"], 
    "DE_LU": ["Germany & Luxembourg", "EUR", "DEU"], "FR": ["France", "EUR", "FRA"], 
    "GB": ["Great Britain", "GBP", "GBR"], "IE_SEM": ["Ireland", "EUR", "IRL"],
    "NL": ["Netherlands", "EUR", "NLD"], "PL": ["Poland", "PLN", "POL"], 
    "DK_1": ["Denmark (West)", "EUR", "DNK"], "DK_2": ["Denmark (East)", "EUR", "DNK"],
    "EE": ["Estonia", "EUR", "EST"], "FI": ["Finland", "EUR", "FIN"], "LT": ["Lithuania", "EUR", "LTU"],
    "LV": ["Latvia", "EUR", "LVA"], "NO_1": ["Norway (NO1)", "EUR", "NOR"], "NO_2": ["Norway (NO2)", "EUR", "NOR"],
    "NO_3": ["Norway (NO3)", "EUR", "NOR"], "NO_4": ["Norway (NO4)", "EUR", "NOR"], "NO_5": ["Norway (NO5)", "EUR", "NOR"],
    "SE_1": ["Sweden (SE1)", "EUR", "SWE"], "SE_2": ["Sweden (SE2)", "EUR", "SWE"], "SE_3": ["Sweden (SE3)", "EUR", "SWE"],
    "SE_4": ["Sweden (SE4)", "EUR", "SWE"], "ES": ["Spain", "EUR", "ESP"], "PT": ["Portugal", "EUR", "PRT"],
    "GR": ["Greece", "EUR", "GRC"], "HR": ["Croatia", "EUR", "HRV"], "HU": ["Hungary", "EUR", "HUN"],
    "RO": ["Romania", "EUR", "ROU"], "RS": ["Serbia", "EUR", "SRB"], "SI": ["Slovenia", "EUR", "SVN"],
    "SK": ["Slovakia", "EUR", "SVK"], "IT_NORD": ["Italy (North)", "EUR", "ITA"]
}

st.set_page_config(page_title="Day-Ahead Market Explorer", layout="wide", page_icon="⚡")

# --- SESSION STATE FOR SYNCING ---
if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)", "Great Britain (GB)"]

# 2. Sidebar (Technical Settings Only)
st.sidebar.header("Technical Settings")
today = datetime.now().date()
date_range = st.sidebar.date_input("Select Date Range", value=(today - timedelta(days=2), today))
resolution = st.sidebar.selectbox("Time Resolution", ["60 min", "15 min"])
res_map = {"60 min": "60min", "15 min": "15min"}

available_codes = sorted(list(ZONE_NAMES.keys()))
display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in available_codes}

# --- MAIN UI LAYOUT ---
st.title("⚡ European Day-Ahead Market Explorer")

# 3. Interactive Map (Visualizer)
def create_map():
    current_selected_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
    map_data = [{"Country": info[0], "ISO": info[2], "Selected": 1 if code in current_selected_codes else 0} for code, info in ZONE_NAMES.items()]
    df_map = pd.DataFrame(map_data)
    
    fig_map = px.choropleth(
        df_map, locations="ISO", color="Selected", hover_name="Country",
        color_continuous_scale=["#f0f0f0", "#1f77b4"], scope="europe"
    )
    fig_map.update_layout(coloraxis_showscale=False, margin={"r":0,"t":0,"l":0,"b":0}, height=350)
    fig_map.update_geos(visible=False, showcountries=True, countrycolor="White")
    return fig_map

st.plotly_chart(create_map(), use_container_width=True)

# 4. Search & Selection (Moved to center for better UX)
col1, col2 = st.columns([4, 1])
with col1:
    selected_labels = st.multiselect(
        "Type to search and add Bidding Zones:", 
        options=sorted(display_options.keys()), 
        key="selected_zones",
        label_visibility="collapsed" # Makes it look like a clean search bar
    )
with col2:
    if st.button("Clear All Selection"):
        st.session_state.selected_zones = []
        st.rerun()

# --- DATA FETCHING & CHARTING ---
if len(date_range) == 2 and st.session_state.selected_zones:
    selected_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
    
    # ... [Same fetch_live_data function as before] ...
    
    with st.spinner('Updating Market Data...'):
        # [Chart and Table logic remains the same as previous version]
        pass
else:
    st.info("Select zones using the search bar above to begin.")

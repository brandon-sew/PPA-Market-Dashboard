import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Comprehensive Mapping
ZONE_NAMES = {
    "AT": ["Austria", "EUR", "AUT"], "BE": ["Belgium", "EUR", "BEL"], "BG": ["Bulgaria", "EUR", "BGR"],
    "CH": ["Switzerland", "EUR", "CHE"], "CZ": ["Czech Republic", "EUR", "CZE"], 
    "DE_LU": ["Germany & Luxembourg", "EUR", "DEU"], "FR": ["France", "EUR", "FRA"], 
    "GB": ["Great Britain", "GBP", "GBR"], "IE_SEM": ["Ireland", "EUR", "IRL"],
    "NL": ["Netherlands", "EUR", "NLD"], "PL": ["Poland", "PLN", "POL"], 
    "DK_1": ["Denmark (DK1)", "EUR", "DNK"], "DK_2": ["Denmark (DK2)", "EUR", "DNK"],
    "EE": ["Estonia", "EUR", "EST"], "FI": ["Finland", "EUR", "FIN"], "LT": ["Lithuania", "EUR", "LTU"],
    "LV": ["Latvia", "EUR", "LVA"], "NO_1": ["Norway (NO1)", "EUR", "NOR"], "NO_2": ["Norway (NO2)", "EUR", "NOR"],
    "NO_3": ["Norway (NO3)", "EUR", "NOR"], "NO_4": ["Norway (NO4)", "EUR", "NOR"], "NO_5": ["Norway (NO5)", "EUR", "NOR"],
    "SE_1": ["Sweden (SE1)", "EUR", "SWE"], "SE_2": ["Sweden (SE2)", "EUR", "SWE"], "SE_3": ["Sweden (SE3)", "EUR", "SWE"],
    "SE_4": ["Sweden (SE4)", "EUR", "SWE"], "BG": ["Bulgaria", "EUR", "BGR"], "ES": ["Spain", "EUR", "ESP"], 
    "GR": ["Greece", "EUR", "GRC"], "HR": ["Croatia", "EUR", "HRV"], "HU": ["Hungary", "EUR", "HUN"],
    "PT": ["Portugal", "EUR", "PRT"], "RO": ["Romania", "EUR", "ROU"], "RS": ["Serbia", "EUR", "SRB"], 
    "SI": ["Slovenia", "EUR", "SVN"], "SK": ["Slovakia", "EUR", "SVK"], "IT_NORD": ["Italy (North)", "EUR", "ITA"],
    "IT_CNOR": ["Italy (CNorth)", "EUR", "ITA"], "IT_CSUD": ["Italy (CSouth)", "EUR", "ITA"],
    "IT_SUD": ["Italy (South)", "EUR", "ITA"], "IT_SICI": ["Italy (Sicily)", "EUR", "ITA"],
    "IT_SARD": ["Italy (Sardinia)", "EUR", "ITA"], "IT_CALA": ["Italy (Calabria)", "EUR", "ITA"]
}

st.set_page_config(page_title="Day-Ahead Market Explorer", layout="wide", page_icon="⚡")

# --- SESSION STATE FOR SELECTION ---
if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)", "Great Britain (GB)"]

# 2. Sidebar (Technical Settings)
st.sidebar.header("Data Settings")
today = datetime.now().date()
date_range = st.sidebar.date_input("Select Date Range", value=(today - timedelta(days=2), today), max_value=today + timedelta(days=1))

resolution = st.sidebar.selectbox("Time Resolution", ["60 min", "15 min"])
res_map = {"60 min": "60min", "15 min": "15min"}

available_codes = sorted(list(ZONE_NAMES.keys()))
display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in available_codes}

# 3. Data Fetching Function
@st.cache_data(ttl=3600)
def fetch_live_data(selected_codes, start_date, end_date):
    if not selected_codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    
    all_data = []
    for code in selected_codes:
        try:
            series = client.query_day_ahead_prices(code, start=start, end=end)
            df_temp = series.to_frame(name='Price').reset_index()
            df_temp.columns = ['Time', 'Price']
            df_temp['Bidding Zone'] = code
            df_temp['Currency'] = ZONE_NAMES[code][1]
            df_temp['Time'] = pd.to_datetime(df_temp['Time'], utc=True)
            all_data.append(df_temp)
        except Exception:
            st.sidebar.error(f"⚠️ {code}: Data unavailable.")
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# --- MAIN UI ---
st.title("⚡ European Day-Ahead Market Explorer")

# MAP SECTION
def create_map():
    current_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
    map_data = [{"Country": info[0], "ISO": info[2], "Selected": 1 if code in current_codes else 0} for code, info in ZONE_NAMES.items()]
    df_map = pd.DataFrame(map_data)
    
    fig_map = px.choropleth(
        df_map, locations="ISO", color="Selected", hover_name="Country",
        color_continuous_scale=["#f0f0f0", "#1f77b4"], scope="europe"
    )
    fig_map.update_layout(coloraxis_showscale=False, margin={"r":0,"t":0,"l":0,"b":0}, height=350)
    fig_map.update_geos(visible=False, showcountries=True, countrycolor="White")
    return fig_map

st.plotly_chart(create_map(), use_container_width=True)

# SEARCH BAR SECTION
col_search, col_btn = st.columns([4, 1])
with col_search:
    # This bar is linked to session state; it updates whenever a country is added/removed
    selected_labels = st.multiselect(
        "Search Bidding Zones:", 
        options=sorted(display_options.keys()), 
        key="selected_zones",
        label_visibility="collapsed"
    )

with col_btn:
    if st.button("Clear All Selection", use_container_width=True):
        st.session_state.selected_zones = []
        st.rerun()

# 4. CHART AND TABLE LOGIC (Restored and Connected)
if len(date_range) == 2 and st.session_state.selected_zones:
    start_dt, end_dt = date_range
    selected_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
    
    with st.spinner('Updating charts and tables...'):
        raw_df = fetch_live_data(selected_codes, start_dt, end_dt)
    
    if not raw_df.empty:
        raw_df['Time'] = raw_df['Time'].dt.tz_convert('Europe/Brussels')
        
        # Resample based on resolution
        plot_df = (
            raw_df.groupby('Bidding Zone')
            .resample(res_map[resolution], on='Time')['Price']
            .mean()
            .reset_index()
        )
        
        plot_df['Currency'] = plot_df['Bidding Zone'].map(lambda x: ZONE_NAMES[x][1])
        plot_df['Display Name'] = plot_df.apply(lambda x: f"{x['Bidding Zone']} ({x['Currency']}/MWh)", axis=1)
        plot_df['Date'] = plot_df['Time'].dt.strftime('%d-%m-%Y')
        plot_df['24h Time'] = plot_df['Time'].dt.strftime('%H:%M')

        # Dynamic Y-axis label logic
        has_non_eur = any(ZONE_NAMES[c][1] != 'EUR' for c in selected_codes)
        y_label = "Price" if has_non_eur else "Price (EUR/MWh)"

        # THE CHART
        fig = px.line(
            plot_df, x='Time', y='Price', color='Display Name',
            labels={'Time': 'Time (CET/CEST)', 'Price': y_label},
            template="plotly_white",
            markers=True if resolution == "60 min" else False,
            hover_data={'Display Name': False, 'Bidding Zone': True, 'Date': True, '24h Time': True, 'Price': ':.2f', 'Time': False}
        )
        fig.update_yaxes(autorange=True, fixedrange=False) # Fix for negative prices
        fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

        # THE DATA TABLE
        st.subheader("Data Table")
        pivot_df = plot_df.pivot_table(index=['Date', '24h Time'], columns='Display Name', values='Price')
        st.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)

    else:
        st.warning("No data found for selected zones.")
else:
    st.info("Search for and select at least one Bidding Zone above to see the chart and data table.")

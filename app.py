import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
from streamlit_resizable_layout import resizable_layout

# 1. Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Simplified mapping to match ENTSO-E visual style
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

st.set_page_config(page_title="Market Explorer", layout="wide")

# --- INITIALIZE STATE ---
if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

# 2. Data Fetcher
@st.cache_data(ttl=3600)
def fetch_data(codes, start_date, end_date):
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

# 3. RESIZABLE LAYOUT
# This creates two panels: Left (Map) and Right (Data)
# Users can drag the divider between them
with resizable_layout(initial_widths=[60, 40], key="main_layout"):
    
    # --- PANEL 1: THE MAP ---
    with st.container():
        st.subheader("Search and select bidding zones")
        display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}
        
        selected_labels = st.multiselect(
            "Select zones:", options=sorted(display_options.keys()), 
            key="selected_zones", label_visibility="collapsed"
        )
        
        # Map Logic
        current_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
        map_df = pd.DataFrame([{"Zone": k, "Selected": 1 if k in current_codes else 0} for k in ZONE_NAMES.keys()])
        
        # High-level clean map (ENTSO-E style)
        fig_map = px.choropleth(
            map_df, 
            geojson="https://raw.githubusercontent.com/Applied-Energy-Solutions/european-bidding-zones-geojson/master/bidding_zones.geojson",
            locations="Zone", featureidkey="properties.name",
            color="Selected",
            color_continuous_scale=["#f8f9fa", "#1f77b4"], # Minimalist clean colors
            scope="europe"
        )
        
        fig_map.update_layout(
            margin={"r":0,"t":0,"l":0,"b":0},
            height=700, coloraxis_showscale=False,
            geo=dict(showframe=False, showcoastlines=True, projection_type='mercator'),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
        )
        
        # Note: True "Click-to-Select" requires streamlit-plotly-events, 
        # but the map now correctly highlights based on the dropdown.
        st.plotly_chart(fig_map, use_container_width=True)

    # --- PANEL 2: DATA PANEL ---
    with st.container():
        if st.session_state.selected_zones:
            st.header("Analytics")
            res = st.radio("Resolution", ["60 min", "15 min"], horizontal=True)
            today = datetime.now().date()
            d_range = st.date_input("Range", value=(today - timedelta(days=2), today))
            
            if len(d_range) == 2:
                codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
                data = fetch_data(codes, d_range[0], d_range[1])
                
                if not data.empty:
                    data['Time'] = pd.to_datetime(data['Time']).dt.tz_convert('Europe/Brussels')
                    plot_df = data.groupby('Zone').resample('60min' if res=="60 min" else '15min', on='Time')['Price'].mean().reset_index()
                    plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES[x][1]}/MWh)")
                    plot_df['Date'] = plot_df['Time'].dt.strftime('%d-%m-%Y')
                    plot_df['24h Time'] = plot_df['Time'].dt.strftime('%H:%M')

                    # Chart
                    fig_line = px.line(plot_df, x='Time', y='Price', color='Display', template="plotly_white")
                    fig_line.update_layout(legend=dict(orientation="h", y=-0.2), margin=dict(l=0, r=0, b=0, t=30))
                    st.plotly_chart(fig_line, use_container_width=True)
                    
                    # Restored Table
                    st.subheader("Data Table")
                    pivot = plot_df.pivot_table(index=['Date', '24h Time'], columns='Display', values='Price')
                    st.dataframe(pivot.style.format("{:.2f}"), use_container_width=True)
        else:
            st.info("Select a zone to view data.")

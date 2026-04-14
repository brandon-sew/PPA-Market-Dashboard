import streamlit as st
import pandas as pd
import plotly.express as px
import os
import requests
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

ZONE_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CH": "Switzerland", 
    "CZ": "Czech Republic", "DE_LU": "Germany & Luxembourg", "EE": "Estonia", 
    "ES": "Spain", "FI": "Finland", "FR": "France", 
    "GB": "Great Britain (Elexon)", 
    "GR": "Greece", "HR": "Croatia", "HU": "Hungary", 
    "IE_SEM": "Ireland & N. Ireland (SEM)", 
    "LT": "Lithuania", "LV": "Latvia", "NL": "Netherlands", "PL": "Poland", 
    "PT": "Portugal", "RO": "Romania", "RS": "Serbia", "SI": "Slovenia", "SK": "Slovakia",
    "DK_1": "Denmark - West", "DK_2": "Denmark - East",
    "NO_1": "Eastern Norway", "NO_2": "Southern Norway", "NO_3": "Central Norway", 
    "NO_4": "Northern Norway", "NO_5": "Western Norway",
    "SE_1": "Northern Sweden", "SE_2": "Central Sweden", "SE_3": "Eastern Sweden", "SE_4": "Southern Sweden",
    "IT_NORD": "Italy - North", "IT_CNOR": "Italy - Central North", "IT_CSUD": "Italy - Central South", 
    "IT_SUD": "Italy - South", "IT_SICI": "Italy - Sicily", "IT_SARD": "Italy - Sardinia"
}

st.set_page_config(page_title="Day-Ahead Market Explorer", layout="wide", page_icon="⚡")

# 2. Sidebar Filters
st.sidebar.header("Data Settings")
today = datetime.now().date()
date_range = st.sidebar.date_input(
    "Select Date Range", 
    value=(today - timedelta(days=2), today),
    max_value=today + timedelta(days=1)
)

resolution = st.sidebar.selectbox("Time Resolution", ["60 min", "15 min"])
res_map = {"60 min": "60min", "15 min": "15min"}

available_codes = sorted(list(ZONE_NAMES.keys()))
display_options = {f"{ZONE_NAMES[c]} ({c.replace('_','')})": c for c in available_codes}

selected_labels = st.sidebar.multiselect(
    "Select Bidding Zones", 
    options=sorted(display_options.keys()), 
    default=["Germany & Luxembourg (DELU)", "Great Britain (Elexon) (GB)"]
)

# 3. Robust Elexon Fetcher
def fetch_gb_elexon(start_date, end_date):
    """Fetches Day-Ahead prices directly from Elexon Insights API."""
    try:
        # Elexon requires YYYY-MM-DD
        s_str = start_date.strftime('%Y-%m-%d')
        e_str = end_date.strftime('%Y-%m-%d')
        
        url = f"https://api.data.elexon.co.uk/insights/v1/market/index/prices?from={s_str}&to={e_str}"
        
        # Headers help prevent 403/500 errors from API gateways
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            st.sidebar.warning(f"Elexon API returned status: {response.status_code}")
            return pd.DataFrame()
            
        data = response.json()
        if not data or len(data) == 0:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        # We need startTime and price
        if 'startTime' in df.columns and 'price' in df.columns:
            df = df[['startTime', 'price']].rename(columns={'startTime': 'Time', 'price': 'Price'})
            df['Time'] = pd.to_datetime(df['Time'], utc=True)
            df['Country'] = 'GB'
            return df
        return pd.DataFrame()
    except Exception as e:
        st.sidebar.error(f"Elexon Fetch Error: {e}")
        return pd.DataFrame()

# 4. Hybrid Fetcher
@st.cache_data(ttl=3600)
def fetch_hybrid_data(selected_codes, start_date, end_date):
    if not selected_codes: return pd.DataFrame()
    all_data = []
    
    for code in selected_codes:
        if code == "GB":
            gb_df = fetch_gb_elexon(start_date, end_date)
            if not gb_df.empty:
                all_data.append(gb_df)
        else:
            try:
                start_ts = pd.Timestamp(start_date, tz='Europe/Brussels')
                end_ts = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
                series = client.query_day_ahead_prices(code, start=start_ts, end=end_ts)
                df_temp = series.to_frame(name='Price').reset_index()
                df_temp.columns = ['Time', 'Price']
                df_temp['Country'] = code.replace("_", "")
                df_temp['Time'] = pd.to_datetime(df_temp['Time'], utc=True)
                all_data.append(df_temp)
            except Exception as e:
                st.sidebar.error(f"⚠️ {ZONE_NAMES.get(code, code)}: {str(e)[:40]}")
            
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# 5. UI Logic
st.title("⚡ European Day-Ahead Electricity Market Prices")

if len(date_range) == 2:
    start_dt, end_dt = date_range
    selected_codes = [display_options[lbl] for lbl in selected_labels]
    
    if selected_codes:
        with st.spinner('Syncing with ENTSO-E and Elexon...'):
            raw_df = fetch_hybrid_data(selected_codes, start_dt, end_dt)
        
        if not raw_df.empty:
            try:
                # Group and resample
                plot_df = (
                    raw_df.groupby('Country')
                    .resample(res_map[resolution], on='Time')['Price']
                    .mean()
                    .reset_index()
                )

                if not plot_df.empty:
                    plot_df['Time'] = plot_df['Time'].dt.tz_convert('Europe/Brussels')
                    fig = px.line(
                        plot_df, x='Time', y='Price', color='Country',
                        labels={'Time': 'Time (CET)', 'Price': 'Price (Local Currency/MWh)'},
                        template="plotly_white"
                    )
                    fig.update_layout(hovermode="x unified")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.subheader("Raw Market Data")
                    st.dataframe(plot_df.pivot(index='Time', columns='Country', values='Price').style.format("{:.2f}"))
            except Exception as e:
                st.error(f"Processing Error: {e}")
        else:
            st.warning("No data found. Check if the markets have cleared yet today.")

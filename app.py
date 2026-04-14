import streamlit as st
import pandas as pd
import plotly.express as px
import os
import requests
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
# Ensure ENTSOE_TOKEN is in your secrets. Elexon Insights v1 is public (no key usually required).
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

ZONE_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CH": "Switzerland", 
    "CZ": "Czech Republic", "DE_LU": "Germany & Luxembourg", "EE": "Estonia", 
    "ES": "Spain", "FI": "Finland", "FR": "France", 
    "GB": "Great Britain", 
    "GR": "Greece", "HR": "Croatia", "HU": "Hungary", 
    "IE_SEM": "Ireland & N. Ireland (SEM)", 
    "LT": "Lithuania", "LV": "Latvia", "NL": "Netherlands", "PL": "Poland", 
    "PT": "Portugal", "RO": "Romania", "RS": "Serbia", "SI": "Slovenia", "SK": "Slovakia",
    "DK_1": "Denmark - West", "DK_2": "Denmark - East",
    "NO_1": "Norway East", "NO_2": "Norway South", "NO_3": "Norway Central", 
    "NO_4": "Norway North", "NO_5": "Norway West",
    "SE_1": "Sweden North", "SE_2": "Sweden Central", "SE_3": "Sweden East", "SE_4": "Sweden South",
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
    default=["Germany & Luxembourg (DELU)", "Great Britain (via Elexon) (GB)"]
)

# 3. Dedicated Elexon Fetcher for GB
def fetch_gb_elexon(start_date, end_date):
    """Fetches Day-Ahead prices directly from Elexon Insights API."""
    try:
        # We use the Market Index Data (which tracks Day-Ahead auctions)
        # Dates are formatted as YYYY-MM-DD
        url = f"https://api.data.elexon.co.uk/insights/v1/market/index/prices?from={start_date}&to={end_date}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        # Note: 'price' is in GBP. We keep it as is or you could multiply by a rate here.
        df = df[['startTime', 'price']].rename(columns={'startTime': 'Time', 'price': 'Price'})
        df['Time'] = pd.to_datetime(df['Time'], utc=True)
        df['Country'] = 'GB'
        return df
    except Exception as e:
        st.sidebar.warning(f"Elexon GB Error: {str(e)[:50]}")
        return pd.DataFrame()

# 4. Hybrid Data Fetching Function
@st.cache_data(ttl=3600)
def fetch_hybrid_data(selected_codes, start_date, end_date):
    if not selected_codes: return pd.DataFrame()
    
    all_data = []
    
    for code in selected_codes:
        if code == "GB":
            # Direct Elexon pull for GB
            gb_df = fetch_gb_elexon(start_date, end_date)
            if not gb_df.empty:
                all_data.append(gb_df)
        else:
            # ENTSO-E pull for everyone else
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
                st.sidebar.error(f"⚠️ {ZONE_NAMES.get(code, code)} (ENTSO-E): {str(e)[:50]}...")
            
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# 5. Main UI Logic
st.title("⚡ European Day-Ahead Electricity Market Prices")

if len(date_range) == 2:
    start_dt, end_dt = date_range
    selected_codes = [display_options[lbl] for lbl in selected_labels]
    
    if not selected_codes:
        st.info("Please select at least one Bidding Zone in the sidebar.")
    else:
        with st.spinner('Accessing Hybrid Market APIs...'):
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
                        plot_df, 
                        x='Time', 
                        y='Price', 
                        color='Country',
                        labels={'Time': 'Time (CET)', 'Price': 'Price (Local Currency/MWh)'},
                        title="Day-Ahead Prices: ENTSO-E (EU) & Elexon (GB)",
                        template="plotly_white",
                        markers=True if resolution == "60 min" else False
                    )
                    fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                    st.plotly_chart(fig, use_container_width=True)

                    st.subheader("Market Data Table")
                    pivot_df = plot_df.pivot(index='Time', columns='Country', values='Price')
                    st.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)
                else:
                    st.warning("Could not process data for this resolution.")
            except Exception as e:
                st.error(f"Display Error: {e}")
        else:
            st.warning("No data found for the selected range.")
else:
    st.info("Please select a valid date range.")

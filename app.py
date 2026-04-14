import streamlit as st
import pandas as pd
import plotly.express as px
import os
import io
import requests
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Your Elexon Portal Scripting Key
ELEXON_KEY = "p5mfp3oei592l2k"

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

# 3. Direct CSV Fetcher for GB
def fetch_gb_csv(start_date, end_date):
    """Fetches the latest Market Index Data (MID) CSV directly from the Elexon Portal."""
    try:
        url = f"https://downloads.elexonportal.co.uk/file/download/LATEST_MID_FILE?key={ELEXON_KEY}"
        
        # Download the file content
        response = requests.get(url, timeout=15)
        
        if response.status_code != 200:
            st.sidebar.warning(f"Elexon CSV download failed: {response.status_code}")
            return pd.DataFrame()
            
        # Read the raw CSV text into a Pandas DataFrame
        # The Elexon CSV format has specific headers, we skip rows until we hit the data
        raw_csv = response.text
        
        # Check if the file is actually a CSV and not an HTML error page
        if "<html" in raw_csv[:50].lower():
            st.sidebar.error("Elexon returned an HTML page instead of CSV. Check your key.")
            return pd.DataFrame()
            
        df = pd.read_csv(io.StringIO(raw_csv), skipinitialspace=True)
        
        # The CSV has columns like 'Settlement Date', 'Settlement Period', 'Price'
        # We need to convert 'Settlement Date' to a real datetime object
        if 'Settlement Date' in df.columns and 'Price' in df.columns:
            # Convert date format (usually DD/MM/YYYY or YYYY-MM-DD in these files)
            df['Settlement Date'] = pd.to_datetime(df['Settlement Date'], format='mixed')
            
            # Filter the dataframe to only include the dates the user requested
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date)
            df = df[(df['Settlement Date'] >= start_ts) & (df['Settlement Date'] <= end_ts)]
            
            # Create a proper 'Time' column by adding the period to the date
            # Assuming 1 period = 30 minutes
            df['Time'] = df.apply(lambda row: row['Settlement Date'] + timedelta(minutes=30 * (row['Settlement Period'] - 1)), axis=1)
            
            # Clean up to match ENTSO-E format
            df = df[['Time', 'Price']].copy()
            df['Time'] = pd.to_datetime(df['Time'], utc=True)
            df['Country'] = 'GB'
            
            # Clean the price column just in case there are strings
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0.0)
            
            return df
            
        return pd.DataFrame()
        
    except Exception as e:
        st.sidebar.error(f"CSV Parse Error: {str(e)[:60]}")
        return pd.DataFrame()

# 4. Hybrid Fetcher
@st.cache_data(ttl=3600)
def fetch_hybrid_data(selected_codes, start_date, end_date):
    if not selected_codes: return pd.DataFrame()
    all_data = []
    for code in selected_codes:
        if code == "GB":
            # Call the new CSV fetcher here!
            gb_df = fetch_gb_csv(start_date, end_date)
            if not gb_df.empty: all_data.append(gb_df)
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
        with st.spinner('Downloading Market Data...'):
            raw_df = fetch_hybrid_data(selected_codes, start_dt, end_dt)
        
        if not raw_df.empty:
            try:
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
                    pivot = plot_df.pivot(index='Time', columns='Country', values='Price')
                    st.dataframe(pivot.style.format("{:.2f}"))
            except Exception as e:
                st.error(f"Display Error: {e}")
        else:
            st.warning("No data found. Make sure the markets have cleared for these dates.")

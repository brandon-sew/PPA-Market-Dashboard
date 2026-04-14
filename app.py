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

# 3. Robust Direct CSV Fetcher for GB
def fetch_gb_csv(start_date, end_date):
    """Robust fetcher for Elexon CSV that auto-detects column names and formats."""
    try:
        url = f"https://downloads.elexonportal.co.uk/file/download/LATEST_MID_FILE?key={ELEXON_KEY}"
        
        # Download the file content
        response = requests.get(url, timeout=15)
        
        if response.status_code != 200:
            st.sidebar.warning(f"Elexon download failed (Status {response.status_code})")
            return pd.DataFrame()
            
        # 1. Read the CSV and clean column names (strip spaces and quotes)
        df = pd.read_csv(io.StringIO(response.text), skipinitialspace=True)
        df.columns = df.columns.str.strip().str.replace('"', '').str.replace("'", "")
        
        # 2. Identify the columns dynamically by keyword
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        price_col = next((c for c in df.columns if 'price' in c.lower()), None)
        period_col = next((c for c in df.columns if 'period' in c.lower()), None)

        if not date_col or not price_col:
            # Diagnostic help if it still fails
            st.sidebar.error(f"GB Column Error. Found: {list(df.columns)}")
            return pd.DataFrame()

        # 3. Convert Dates and Prices using UK formats
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
        
        # 4. Filter for the date range selected in the sidebar
        mask = (df[date_col].dt.date >= start_date) & (df[date_col].dt.date <= end_date)
        df = df.loc[mask].copy()

        if df.empty:
            return pd.DataFrame()

        # 5. Create proper Time column
        if period_col:
            # Handle half-hour periods (1-48)
            df['Time'] = df.apply(
                lambda x: x[date_col] + timedelta(minutes=30 * (int(x[period_col]) - 1)), 
                axis=1
            )
        else:
            df['Time'] = df[date_col]

        # 6. Final Formatting to match chart requirements
        df = df[['Time', price_col]].rename(columns={price_col: 'Price'})
        df['Time'] = pd.to_datetime(df['Time'], utc=True)
        df['Country'] = 'GB'
        
        return df
        
    except Exception as e:
        st.sidebar.error(f"GB Process Error: {str(e)[:60]}")
        return pd.DataFrame()

# 4. Hybrid Fetcher
@st.cache_data(ttl=3600)
def fetch_hybrid_data(selected_codes, start_date, end_date):
    if not selected_codes: return pd.DataFrame()
    all_data = []
    for code in selected_codes:
        if code == "GB":
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
                    # Adjust time for display
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
            st.warning("No data found for the selected range. Check if the markets have cleared yet.")

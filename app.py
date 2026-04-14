import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# UPDATED: Mapping for GB and SEM
ZONE_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CH": "Switzerland", 
    "CZ": "Czech Republic", "DE_LU": "Germany & Luxembourg", "EE": "Estonia", 
    "ES": "Spain", "FI": "Finland", "FR": "France", 
    "GB": "Great Britain", # Main GB Bidding Zone
    "GR": "Greece", "HR": "Croatia", "HU": "Hungary", 
    "IE_SEM": "Ireland & N. Ireland (SEM)", # Single Electricity Market
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
    default=[f"Germany & Luxembourg (DELU)", f"Great Britain (GB)"]
)

# 3. Data Fetching Function
@st.cache_data(ttl=3600)
def fetch_live_data(selected_codes, start_date, end_date):
    if not selected_codes: return pd.DataFrame()
    
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    
    all_data = []
    for code in selected_codes:
        try:
            # For some users, ENTSO-E requires 'GB' to be explicitly handled
            # but the standard query_day_ahead_prices usually works.
            series = client.query_day_ahead_prices(code, start=start, end=end)
            df_temp = series.to_frame(name='Price').reset_index()
            df_temp.columns = ['Time', 'Price']
            df_temp['Country'] = code.replace("_", "")
            df_temp['Time'] = pd.to_datetime(df_temp['Time'], utc=True)
            all_data.append(df_temp)
        except Exception as e:
            st.sidebar.error(f"⚠️ {ZONE_NAMES.get(code, code)}: {str(e)[:50]}...")
            
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# 4. Main UI Logic
st.title("⚡ European Day-Ahead Electricity Market Prices")

if len(date_range) == 2:
    start_dt, end_dt = date_range
    selected_codes = [display_options[lbl] for lbl in selected_labels]
    
    if not selected_codes:
        st.info("Please select at least one Bidding Zone in the sidebar.")
    else:
        with st.spinner('Accessing ENTSO-E API...'):
            raw_df = fetch_live_data(selected_codes, start_dt, end_dt)
        
        if not raw_df.empty:
            try:
                # Column-based resampling for stability
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
                        labels={'Time': 'Time (CET)', 'Price': 'Price (EUR/MWh)'},
                        template="plotly_white",
                        markers=True if resolution == "60 min" else False
                    )
                    fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                    st.plotly_chart(fig, use_container_width=True)

                    st.subheader("Market Comparison Table")
                    pivot_df = plot_df.pivot(index='Time', columns='Country', values='Price')
                    st.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)
                else:
                    st.warning("Data fetched but could not be processed into the requested resolution.")
            except Exception as e:
            # This will show the actual error (e.g., "404 No Data Found" or "Unauthorized")
            st.sidebar.error(f"⚠️ {ZONE_NAMES.get(code, code)}: {e}")
        else:
            st.warning("No data found. Ensure your API key is correct and tomorrow's prices have been published (post-13:00 CET).")

import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Comprehensive Mapping based on ENTSO-E List View
ZONE_NAMES = {
    # Central & Western Europe
    "AT": "Austria",
    "BE": "Belgium",
    "CH": "Switzerland",
    "CZ": "Czech Republic",
    "DE_LU": "Germany & Luxembourg",
    "FR": "France",
    "GB": "Great Britain",
    "IE_SEM": "Ireland (SEM)",
    "NL": "Netherlands",
    "PL": "Poland",

    # Northern Europe
    "DK_1": "Denmark (DK1)",
    "DK_2": "Denmark (DK2)",
    "EE": "Estonia",
    "FI": "Finland",
    "LT": "Lithuania",
    "LV": "Latvia",
    "NO_1": "Norway (NO1)",
    "NO_2": "Norway (NO2)",
    "NO_3": "Norway (NO3)",
    "NO_4": "Norway (NO4)",
    "NO_5": "Norway (NO5)",
    "SE_1": "Sweden (SE1)",
    "SE_2": "Sweden (SE2)",
    "SE_3": "Sweden (SE3)",
    "SE_4": "Sweden (SE4)",

    # Southern & Eastern Europe
    "BG": "Bulgaria",
    "ES": "Spain",
    "GR": "Greece",
    "HR": "Croatia",
    "HU": "Hungary",
    "PT": "Portugal",
    "RO": "Romania",
    "RS": "Serbia",
    "SI": "Slovenia",
    "SK": "Slovakia",

    # Italy (Comprehensive Zone List)
    "IT_NORD": "Italy (North)",
    "IT_CNOR": "Italy (Central North)",
    "IT_CSUD": "Italy (Central South)",
    "IT_SUD": "Italy (South)",
    "IT_SICI": "Italy (Sicily)",
    "IT_SARD": "Italy (Sardinia)",
    "IT_CALA": "Italy (Calabria)",
    "IT_SACO_AC": "Italy (Saco AC)",
    "IT_SACO_DC": "Italy (Saco DC)",
    "IT_BRNN": "Italy (Brindisi)",
    "IT_FOGN": "Italy (Foggia)",
    "IT_ROSN": "Italy (Rossano)",
    "IT_PRGP": "Italy (Priolo Gargallo)",
    "IT_MALT": "Italy (Malta)"
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
display_options = {f"{ZONE_NAMES[c]} ({c})": c for c in available_codes}

# Corrected default to use the underscore version to match keys
selected_labels = st.sidebar.multiselect(
    "Select Bidding Zones", 
    options=sorted(display_options.keys()), 
    default=["Germany & Luxembourg (DE_LU)", "Great Britain (GB)"]
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
            series = client.query_day_ahead_prices(code, start=start, end=end)
            df_temp = series.to_frame(name='Price').reset_index()
            df_temp.columns = ['Time', 'Price']
            df_temp['Country'] = code
            df_temp['Time'] = pd.to_datetime(df_temp['Time'], utc=True)
            all_data.append(df_temp)
        except Exception as e:
            # Errors appear in sidebar so they don't break the main chart
            st.sidebar.error(f"⚠️ {code}: Data unavailable or not yet released.")
            
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
                # Group and resample for time consistency
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
                        labels={'Time': 'Time (CET/CEST)', 'Price': 'Price (Local Currency/MWh)'},
                        template="plotly_white",
                        markers=True if resolution == "60 min" else False
                    )
                    fig.update_layout(
                        hovermode="x unified", 
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    st.subheader("Market Comparison Table")
                    pivot_df = plot_df.pivot(index='Time', columns='Country', values='Price')
                    st.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)
                else:
                    st.warning("Data was fetched but couldn't be processed.")
            
            except Exception as e:
                st.error(f"Display Error: {e}")
        else:
            st.warning("No data found for the selected range. Note: Prices for the next day are usually released between 12:45 and 13:30 CET.")
else:
    st.info("Please select a date range (Start and End date) in the sidebar.")

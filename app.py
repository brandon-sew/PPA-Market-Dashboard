import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Mapping with Currency Info
ZONE_NAMES = {
    "AT": ["Austria", "EUR"], "BE": ["Belgium", "EUR"], "CH": ["Switzerland", "EUR"],
    "CZ": ["Czech Republic", "EUR"], "DE_LU": ["Germany & Luxembourg", "EUR"],
    "FR": ["France", "EUR"], "GB": ["Great Britain", "GBP"], "IE_SEM": ["Ireland", "EUR"],
    "NL": ["Netherlands", "EUR"], "PL": ["Poland", "PLN"], "DK_1": ["Denmark (DK1)", "EUR"],
    "DK_2": ["Denmark (DK2)", "EUR"], "EE": ["Estonia", "EUR"], "FI": ["Finland", "EUR"],
    "LT": ["Lithuania", "EUR"], "LV": ["Latvia", "EUR"], "NO_1": ["Norway (NO1)", "EUR"],
    "NO_2": ["Norway (NO2)", "EUR"], "NO_3": ["Norway (NO3)", "EUR"],
    "NO_4": ["Norway (NO4)", "EUR"], "NO_5": ["Norway (NO5)", "EUR"],
    "SE_1": ["Sweden (SE1)", "EUR"], "SE_2": ["Sweden (SE2)", "EUR"],
    "SE_3": ["Sweden (SE3)", "EUR"], "SE_4": ["Sweden (SE4)", "EUR"],
    "BG": ["Bulgaria", "EUR"], "ES": ["Spain", "EUR"], "GR": ["Greece", "EUR"],
    "HR": ["Croatia", "EUR"], "HU": ["Hungary", "EUR"], "PT": ["Portugal", "EUR"],
    "RO": ["Romania", "EUR"], "RS": ["Serbia", "EUR"], "SI": ["Slovenia", "EUR"],
    "SK": ["Slovakia", "EUR"], "IT_NORD": ["Italy (North)", "EUR"],
    "IT_CNOR": ["Italy (CNorth)", "EUR"], "IT_CSUD": ["Italy (CSouth)", "EUR"],
    "IT_SUD": ["Italy (South)", "EUR"], "IT_SICI": ["Italy (Sicily)", "EUR"],
    "IT_SARD": ["Italy (Sardinia)", "EUR"], "IT_CALA": ["Italy (Calabria)", "EUR"]
}

st.set_page_config(page_title="Day-Ahead Market Explorer", layout="wide", page_icon="⚡")

# 2. Sidebar Filters
st.sidebar.header("Data Settings")
today = datetime.now().date()
date_range = st.sidebar.date_input("Select Date Range", value=(today - timedelta(days=2), today), max_value=today + timedelta(days=1))

resolution = st.sidebar.selectbox("Time Resolution", ["60 min", "15 min"])
res_map = {"60 min": "60min", "15 min": "15min"}

available_codes = sorted(list(ZONE_NAMES.keys()))
display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in available_codes}

selected_labels = st.sidebar.multiselect(
    "Select Bidding Zones", 
    options=sorted(display_options.keys()), 
    default=[f"{ZONE_NAMES['DE_LU'][0]} (DE_LU)", f"{ZONE_NAMES['GB'][0]} (GB)"]
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
            df_temp['Bidding Zone'] = code
            df_temp['Currency'] = ZONE_NAMES[code][1]
            df_temp['Time'] = pd.to_datetime(df_temp['Time'], utc=True)
            all_data.append(df_temp)
        except Exception as e:
            st.sidebar.error(f"⚠️ {code}: Data unavailable.")
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# 4. Main UI Logic
st.title("⚡ European Day-Ahead Electricity Market Prices")

if len(date_range) == 2:
    start_dt, end_dt = date_range
    selected_codes = [display_options[lbl] for lbl in selected_labels]
    
    if selected_codes:
        with st.spinner('Accessing ENTSO-E API...'):
            raw_df = fetch_live_data(selected_codes, start_dt, end_dt)
        
        if not raw_df.empty:
            # Time Conversion
            raw_df['Time'] = raw_df['Time'].dt.tz_convert('Europe/Brussels')
            
            # 2) Resampling Logic for Table & Chart
            # We group by zone first, then resample to ensure 60min/15min is respected
            plot_df = (
                raw_df.groupby('Bidding Zone')
                .resample(res_map[resolution], on='Time')['Price']
                .mean() # Correctly averages 15m into 60m if needed
                .reset_index()
            )
            
            # Re-merge the currency info after resampling
            plot_df['Currency'] = plot_df['Bidding Zone'].map(lambda x: ZONE_NAMES[x][1])
            plot_df['Display Name'] = plot_df.apply(lambda x: f"{x['Bidding Zone']} ({x['Currency']}/MWh)", axis=1)
            plot_df['Date'] = plot_df['Time'].dt.strftime('%d-%m-%Y')
            plot_df['24h Time'] = plot_df['Time'].dt.strftime('%H:%M')

            # 1) Dynamic Y-Axis Label Logic
            has_non_eur = any(ZONE_NAMES[c][1] != 'EUR' for c in selected_codes)
            y_label = "Price" if has_non_eur else "Price (EUR/MWh)"

            # 6) Chart with Automatic Negative Axis handling
            fig = px.line(
                plot_df, x='Time', y='Price', color='Display Name',
                labels={'Time': 'Time (CET/CEST)', 'Price': y_label},
                template="plotly_white",
                markers=True if resolution == "60 min" else False,
                hover_data={
                    'Display Name': False,
                    'Bidding Zone': True,
                    'Date': True,
                    '24h Time': True,
                    'Price': ':.2f',
                    'Time': False 
                }
            )
            
            # 3) Ensure Y-axis is NOT fixed at 0 (allows negative prices)
            fig.update_yaxes(autorange=True, fixedrange=False)
            
            fig.update_layout(
                hovermode="x unified", 
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig, use_container_width=True)

            # 2, 3, 4, 5) Data Table (Now respects resampling)
            st.subheader("Data Table")
            pivot_df = plot_df.pivot_table(index=['Date', '24h Time'], columns='Display Name', values='Price')
            st.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)
        else:
            st.warning("No data found.")

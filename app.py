import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Setup & Config
# Ensure you have ENTSOE_TOKEN in your Streamlit Cloud Secrets!
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Full Bidding Zone Dictionary with your Custom Names
ZONE_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CH": "Switzerland", 
    "CZ": "Czech Republic", "DE": "Germany", "EE": "Estonia", "ES": "Spain", 
    "FI": "Finland", "FR": "France", "GB": "Great Britain", "GR": "Greece", 
    "HR": "Croatia", "HU": "Hungary", "IE_SEM": "Ireland (SEM)", "LT": "Lithuania", 
    "LU": "Luxembourg", "LV": "Latvia", "NL": "Netherlands", "PL": "Poland", 
    "PT": "Portugal", "RO": "Romania", "RS": "Serbia", "SI": "Slovenia", "SK": "Slovakia",
    # Denmark
    "DK_1": "Denmark - West", "DK_2": "Denmark - East",
    # Your Custom Norway Names
    "NO_1": "Eastern Norway", "NO_2": "Southern Norway", "NO_3": "Central Norway", 
    "NO_4": "Northern Norway", "NO_5": "Western Norway",
    # Your Custom Sweden Names
    "SE_1": "Northern Sweden", "SE_2": "Central Sweden", "SE_3": "Eastern Sweden", "SE_4": "Southern Sweden",
    # Italy Detailed
    "IT_NORD": "Italy - North", "IT_CNOR": "Italy - Central North", "IT_CSUD": "Italy - Central South", 
    "IT_SUD": "Italy - South", "IT_SICI": "Italy - Sicily", "IT_SARD": "Italy - Sardinia"
}

st.set_page_config(page_title="Day-Ahead Market Explorer", layout="wide", page_icon="⚡")

# 2. Sidebar Filters
st.sidebar.header("Data Settings")

# Date Range Selector
today = datetime.now().date()
date_range = st.sidebar.date_input(
    "Select Date Range", 
    value=(today - timedelta(days=2), today),
    max_value=today + timedelta(days=1)
)

# Resolution Selector
resolution = st.sidebar.selectbox("Time Resolution", ["60 min", "15 min"])
res_map = {"60 min": "60min", "15 min": "15min"}

# Country Multiselect
available_codes = sorted(list(ZONE_NAMES.keys()))
display_options = {f"{ZONE_NAMES[c]} ({c.replace('_','')})": c for c in available_codes}
selected_labels = st.sidebar.multiselect(
    "Select Bidding Zones", 
    options=sorted(display_options.keys()), 
    default=[f"Germany (DE)", f"France (FR)"]
)

# 3. Data Fetching Function
@st.cache_data(ttl=3600)
def fetch_live_data(selected_codes, start_date, end_date):
    if not selected_codes: return pd.DataFrame()
    
    # ENTSO-E needs localized timestamps
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    
    all_data = []
    for code in selected_codes:
        try:
            series = client.query_day_ahead_prices(code, start=start, end=end)
            df_temp = series.to_frame(name='Price')
            df_temp['Country'] = code.replace("_", "")
            all_data.append(df_temp)
        except Exception as e:
            st.sidebar.warning(f"No data for {code}: {e}")
            
    return pd.concat(all_data) if all_data else pd.DataFrame()

# 4. Main UI Logic
st.title("⚡ European Day-Ahead Electricity Market Prices")
st.markdown(f"Currently viewing prices at **{resolution}** resolution.")

if len(date_range) == 2:
    start_dt, end_dt = date_range
    selected_codes = [display_options[lbl] for lbl in selected_labels]
    
    with st.spinner('Fetching market data...'):
        raw_df = fetch_live_data(selected_codes, start_dt, end_dt)
    
    if not raw_df.empty:
        # Resample data based on user selection
        # Note: resample().mean() handles cases where 15-min data is requested from 60-min sources
        plot_df = raw_df.groupby('Country')['Price'].resample(res_map[resolution]).mean().reset_index()

        # Line Chart
        fig = px.line(
            plot_df, 
            x='index', 
            y='Price', 
            color='Country',
            labels={'index': 'Time (CET)', 'Price': 'Price (EUR/MWh)'},
            template="plotly_white",
            markers=True if resolution == "60 min" else False
        )
        fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

        # Side-by-Side Pivot Table
        st.subheader("Raw Data Export")
        pivot_df = plot_df.pivot(index='index', columns='Country', values='Price')
        st.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)
        
        # Download
        csv = plot_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download filtered data as CSV", data=csv, file_name="day_ahead_prices.csv", mime="text/csv")
    else:
        st.info("No data found for this selection. Try adjusting the date range or bidding zones.")
else:
    st.info("Please select a complete start and end date in the sidebar.")

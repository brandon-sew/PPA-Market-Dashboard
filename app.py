import streamlit as st
import pandas as pd
import plotly.express as px

# 1. Bidding Zone Name Dictionary with your custom Norway/Sweden names
ZONE_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CH": "Switzerland", 
    "CZ": "Czech Republic", "DE": "Germany", "EE": "Estonia", "ES": "Spain", 
    "FI": "Finland", "FR": "France", "GB": "Great Britain", "GR": "Greece", 
    "HR": "Croatia", "HU": "Hungary", "IE_SEM": "Ireland (SEM)", "LT": "Lithuania", 
    "LU": "Luxembourg", "LV": "Latvia", "NL": "Netherlands", "PL": "Poland", 
    "PT": "Portugal", "RO": "Romania", "RS": "Serbia", "SI": "Slovenia", "SK": "Slovakia",
    # Regional Splits with Custom Labels
    "DK_1": "Denmark - West", "DK_2": "Denmark - East",
    "NO_1": "Eastern Norway", "NO_2": "Southern Norway", "NO_3": "Central Norway", 
    "NO_4": "Northern Norway", "NO_5": "Western Norway",
    "SE_1": "Northern Sweden", "SE_2": "Central Sweden", "SE_3": "Eastern Sweden", "SE_4": "Southern Sweden",
    "IT_NORD": "Italy - North", "IT_CNOR": "Italy - Central North", "IT_CSUD": "Italy - Central South", 
    "IT_SUD": "Italy - South", "IT_SICI": "Italy - Sicily", "IT_SARD": "Italy - Sardinia"
}

st.set_page_config(page_title="Day-Ahead Market Tracker", layout="wide")

@st.cache_data
def load_data():
    try:
        df = pd.read_csv('market_prices.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        return df
    except: return None

df = load_data()

# Accurate Title for Day-Ahead Markets
st.title("⚡ European Day-Ahead Electricity Market Prices")
st.markdown("Automated Monitoring of Baseload, Peak, and Off-Peak Auction Results")

if df is not None:
    # 2. Prepare Display Labels (Removing Underscores for clean look)
    available_codes = df['Country'].unique()
    
    # Map the "Friendly Name (Code)" to the original code
    display_to_code = {}
    for c in available_codes:
        clean_code = c.replace("_", "") # NO_1 becomes NO1
        full_name = ZONE_NAMES.get(c, "Unknown Region")
        label = f"{full_name} ({clean_code})"
        display_to_code[label] = c
    
    options = sorted(display_to_code.keys())

    # 3. Sidebar Controls for Comparison
    st.sidebar.header("Comparison Settings")
    selected_labels = st.sidebar.multiselect(
        "Select Bidding Zones to Compare", 
        options=options, 
        default=options[0] if options else None
    )
    
    selected_metrics = st.sidebar.multiselect(
        "Select Metrics", 
        options=df['Metric'].unique(), 
        default=['Baseload']
    )

    # 4. Filter and Transform Data
    selected_codes = [display_to_code[lbl] for lbl in selected_labels]
    mask = (df['Country'].isin(selected_codes)) & (df['Metric'].isin(selected_metrics))
    filtered_df = df[mask].copy()
    
    # Create legend label: "Country Code - Metric"
    filtered_df['DisplayGroup'] = filtered_df['Country'].replace("_", "", regex=True) + " - " + filtered_df['Metric']

    # 5. Visualizations
    if not filtered_df.empty:
        # Comparison Line Chart
        fig = px.line(
            filtered_df.sort_values('Date'), 
            x='Date', 
            y='Price', 
            color='DisplayGroup',
            labels={'Price': 'Price (EUR/MWh)', 'Date': '', 'DisplayGroup': 'Market / Metric'},
            markers=True,
            template="plotly_white",
            title="Market Comparison Trend"
        )
        # Unified hover makes comparing specific days much easier
        fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

        # Side-by-Side Pivot Table
        st.subheader("Price Breakdown")
        pivot_df = filtered_df.pivot(index='Date', columns='DisplayGroup', values='Price')
        st.dataframe(pivot_df.style.format("€{:.2f}"), use_container_width=True)
    else:
        st.info("Please select at least one bidding zone and one metric in the sidebar to start comparing.")

else:
    st.error("Market data file not found. Check if the GitHub Action successfully generated 'market_prices.csv'.")

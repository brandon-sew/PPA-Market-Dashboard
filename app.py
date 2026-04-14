import streamlit as st
import pandas as pd
import plotly.express as px

# 1. Bidding Zone Name Dictionary
ZONE_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CH": "Switzerland", 
    "CZ": "Czech Republic", "DE": "Germany", "EE": "Estonia", "ES": "Spain", 
    "FI": "Finland", "FR": "France", "GB": "Great Britain", "GR": "Greece", 
    "HR": "Croatia", "HU": "Hungary", "IE_SEM": "Ireland (SEM)", "LT": "Lithuania", 
    "LU": "Luxembourg", "LV": "Latvia", "NL": "Netherlands", "PL": "Poland", 
    "PT": "Portugal", "RO": "Romania", "RS": "Serbia", "SI": "Slovenia", "SK": "Slovakia",
    # Regional Splits (Denmark, Norway, Sweden, Italy)
    "DK_1": "Denmark - West", "DK_2": "Denmark - East",
    "NO_1": "Norway - Oslo", "NO_2": "Norway - Kristiansand", "NO_3": "Norway - Trondheim", 
    "NO_4": "Norway - Tromsø", "NO_5": "Norway - Bergen",
    "SE_1": "Sweden - Luleå", "SE_2": "Sweden - Sundsvall", "SE_3": "Sweden - Stockholm", "SE_4": "Sweden - Malmö",
    "IT_NORD": "Italy - North", "IT_CNOR": "Italy - Central North", "IT_CSUD": "Italy - Central South", 
    "IT_SUD": "Italy - South", "IT_SICI": "Italy - Sicily", "IT_SARD": "Italy - Sardinia"
}

st.set_page_config(page_title="EU Energy Market", layout="wide")

@st.cache_data
def load_data():
    try:
        df = pd.read_csv('market_prices.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        return df
    except: return None

df = load_data()

st.title("🇪🇺 European Energy Market Tracker")

if df is not None:
    # 2. Prepare Display Labels (Removing Underscores)
    available_codes = df['Country'].unique()
    
    # Create list of "Name (Cleaned Code)"
    display_options = []
    code_to_label = {}
    for code in available_codes:
        clean_code = code.replace("_", "") # NO_1 becomes NO1
        full_name = ZONE_NAMES.get(code, "Unknown Region")
        label = f"{full_name} ({clean_code})"
        display_options.append(label)
        code_to_label[label] = code # Map it back for filtering

    selected_label = st.sidebar.selectbox("Select Bidding Zone", sorted(display_options))
    selected_country_code = code_to_label[selected_label]
    
    selected_metrics = st.sidebar.multiselect("Metrics", options=df['Metric'].unique(), default=['Baseload', 'Peak'])

    # 3. Filter and Graph
    mask = (df['Country'] == selected_country_code) & (df['Metric'].isin(selected_metrics))
    filtered_df = df[mask].sort_values(by='Date')

    col1, col2 = st.columns([3, 1])
    with col1:
        fig = px.line(filtered_df, x='Date', y='Price', color='Metric', markers=True, template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Latest Prices")
        latest_date = filtered_df['Date'].max()
        st.dataframe(filtered_df[filtered_df['Date'] == latest_date][['Metric', 'Price']], hide_index=True)

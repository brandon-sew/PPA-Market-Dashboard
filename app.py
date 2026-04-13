import streamlit as st
import pandas as pd
import plotly.express as px

# 1. Page Configuration
st.set_page_config(
    page_title="EU Energy Market Dashboard",
    page_icon="⚡",
    layout="wide"
)

# 2. Load Data
# This looks for the CSV created by your GitHub Action
@st.cache_data
def load_data():
    try:
        df = pd.read_csv('market_prices.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        return df
    except FileNotFoundError:
        return None

df = load_data()

# 3. Header
st.title("🇪🇺 European Energy Market Tracker")
st.markdown("Automated daily updates for Baseload, Peak, and Off-Peak prices.")

if df is None:
    st.error("No data found. Please run the GitHub Action (main.py) to generate the market_prices.csv file.")
else:
    # 4. Sidebar Filters
    st.sidebar.header("Dashboard Controls")
    
    countries = sorted(df['Country'].unique())
    selected_country = st.sidebar.selectbox("Select Bidding Zone", countries)
    
    metrics = df['Metric'].unique()
    selected_metrics = st.sidebar.multiselect("Select Metrics", 
                                            options=metrics, 
                                            default=['Baseload', 'Peak'])

    # Filter data
    mask = (df['Country'] == selected_country) & (df['Metric'].isin(selected_metrics))
    filtered_df = df[mask].sort_values(by='Date')

    # 5. Visualizations
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader(f"Price Trends: {selected_country}")
        if not filtered_df.empty:
            fig = px.line(
                filtered_df, 
                x='Date', 
                y='Price', 
                color='Metric',
                labels={'Price': 'Price (EUR/MWh)', 'Date': ''},
                markers=True,
                template="plotly_white"
            )
            # Make the chart look cleaner
            fig.update_layout(hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Please select at least one metric.")

    with col2:
        st.subheader("Latest Prices")
        # Get the most recent date in the dataset
        latest_date = filtered_df['Date'].max()
        latest_data = filtered_df[filtered_df['Date'] == latest_date]
        
        if not latest_data.empty:
            st.info(f"Showing data for: {latest_date.strftime('%Y-%m-%d')}")
            # Formatted table
            st.dataframe(
                latest_data[['Metric', 'Price']].style.format({"Price": "€{:.2f}"}),
                hide_index=True,
                use_container_width=True
            )
        
        st.subheader("Historical View")
        st.dataframe(
            filtered_df[['Date', 'Metric', 'Price']].style.format({"Price": "{:.2f}"}),
            hide_index=True,
            height=300
        )

# 6. Footer
st.divider()
st.caption("Data Source: ENTSO-E Transparency Platform. Updates automatically via GitHub Actions.")

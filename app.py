import streamlit as st
import pandas as pd
import plotly.express as px
import os
import json
import glob
import numpy as np
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from concurrent.futures import ThreadPoolExecutor, as_completed
#NEW ENTSOE-E MARKET FEED
import feedparser

# 1. Config & API Setup
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

ZONE_NAMES = {
    "AT": ["Austria", "EUR"], "BE": ["Belgium", "EUR"], "BG": ["Bulgaria", "EUR"],
    "CH": ["Switzerland", "EUR"], "CZ": ["Czech Republic", "EUR"], 
    "DE_LU": ["Germany & Luxembourg", "EUR"], "FR": ["France", "EUR"], 
    "GB": ["Great Britain", "GBP"], "IE_SEM": ["Ireland", "EUR"],
    "NL": ["Netherlands", "EUR"], "PL": ["Poland", "PLN"], 
    "DK_1": ["Denmark West", "EUR"], "DK_2": ["Denmark East", "EUR"],
    "EE": ["Estonia", "EUR"], "FI": ["Finland", "EUR"], "LT": ["Lithuania", "EUR"],
    "LV": ["Latvia", "EUR"], "NO_1": ["Norway East", "EUR"], "NO_2": ["Norway South", "EUR"],
    "NO_3": ["Norway Central", "EUR"], "NO_4": ["Norway Northern", "EUR"], "NO_5": ["Norway West", "EUR"],
    "SE_1": ["Sweden Luleå", "EUR"], "SE_2": ["Sweden Sundsvall", "EUR"], "SE_3": ["Sweden Stockholm", "EUR"],
    "SE_4": ["Sweden Malmö", "EUR"], "ES": ["Spain", "EUR"], "PT": ["Portugal", "EUR"],
    # Balkan and Central/Eastern European Zones
    "HR": ["Croatia", "EUR"], "HU": ["Hungary", "EUR"], 
    "ME": ["Montenegro", "EUR",], "MK": ["North Macedonia", "EUR"],
    "RO": ["Romania", "EUR"], "RS": ["Serbia", "EUR"], 
    "SI": ["Slovenia", "EUR"], "SK": ["Slovakia", "EUR"],
    # Italian Zones
    "IT_NORD": ["Italy North", "EUR"], "IT_CNOR": ["Italy Central North", "EUR"],
    "IT_CSUD": ["Italy Central South", "EUR"], "IT_SUD": ["Italy South", "EUR"],
    "IT_SICI": ["Italy Sicily", "EUR"], "IT_SARD": ["Italy Sardinia", "EUR"], "IT_CALA": ["Italy Calabria", "EUR"]
}

st.set_page_config(page_title="Market Explorer", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    section[data-testid="stSidebar"] { width: 400px !important; }
    .main .block-container { 
        padding-top: 2rem !important;
        max-width: 98% !important; 
    }
    </style>
    """, unsafe_allow_html=True)

if 'selected_zones' not in st.session_state:
    st.session_state.selected_zones = ["Germany & Luxembourg (DE_LU)"]

with st.sidebar:
    st.title("Configuration")
    display_options = {f"{ZONE_NAMES[c][0]} ({c})": c for c in ZONE_NAMES.keys()}
    
    # FIX: Use 'default' instead of 'key' to allow programmatic updates from the map
    chosen_from_dropdown = st.multiselect("Select bidding zones:", 
                                         options=sorted(display_options.keys()), 
                                         default=st.session_state.selected_zones)
    
    # Sync session state if the user manually interacts with the dropdown
    if chosen_from_dropdown != st.session_state.selected_zones:
        st.session_state.selected_zones = chosen_from_dropdown
        st.rerun()
    
    # --- MOVED DROPDOWN ---
    gen_options = ["Solar", "Wind Onshore", "Wind Offshore"]
    selected_gen_types = st.multiselect("Overlay Generation Forecast:", options=gen_options)

    st.divider()
    exclude_neg = st.checkbox("No Settlement for Negative Prices", help="Treats negative prices as 0 for capture price calculation")
    st.divider()
    res = st.radio("Resolution", ["60 min", "15 min"], horizontal=True)
    today = datetime.now().date()
    d_range = st.date_input("Date Range", value=(today - timedelta(days=2), today))
    #NEW GEOPOLITICAL NEWS SECTION
    st.divider()
    st.subheader("Market Intelligence")
    energy_news_url = "https://www.reuters.com/arc/outboundfeeds/rss/concepts/energy/"
    with st.expander("Latest Montel News Updates", expanded=True):
        try:
            #Parse the feed
            feed = feedparser.parse(energy_news_url)
            #Display the top 5 most recent articles
            for entry in feed.entries[:3]:
                #format the date string (e.g. "Wed, 29th April 2026")
                date_str = " ".join(entry.published.split()[:4])
                st.markdown(f"**{date_str}**")
                st.markdown(f"[{entry.title}]({entry.link})")
                if 'summary' in entry:
                    #Clean up HTML tags if any
                    summary = entry.summary.split('<')[0][:100] + "..."
                    st.caption(summary)
                st.divider()
        except Exception:
            st.error("News feed currently unavailable.")

@st.cache_data(ttl=3600)
def fetch_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    
    def get_price(code):
        try:
            series = client.query_day_ahead_prices(code, start=start, end=end)
            df = series.to_frame(name='Price').reset_index()
            df.columns = ['Time', 'Price']
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            return df
        except: return None

    all_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_price, code) for code in codes]
        for future in as_completed(futures):
            res = future.result()
            if res is not None: all_data.append(res)
            
    return pd.concat(all_data) if all_data else pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_gen_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)

    def get_gen(code):
        try:
            df = client.query_generation(code, start=start, end=end)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.T.groupby(level=0).sum().T 
            df = df.reset_index().rename(columns={'index': 'Time'})
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            return df
        except: return None

    all_gen = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_gen, code) for code in codes]
        for future in as_completed(futures):
            res = future.result()
            if res is not None: all_gen.append(res)
            
    return pd.concat(all_gen) if all_gen else pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_forecast_data(codes, start_date, end_date):
    if not codes: return pd.DataFrame()
    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)
    
    def get_forecast(code):
        try:
            df = client.query_wind_and_solar_forecast(code, start=start, end=end)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.T.groupby(level=0).sum().T 
            df = df.reset_index().rename(columns={'index': 'Time'})
            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')
            df['Zone'] = code
            return df
        except: return None

    all_forecast = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_forecast, code) for code in codes]
        for future in as_completed(futures):
            res = future.result()
            if res is not None: all_forecast.append(res)
            
    return pd.concat(all_forecast) if all_forecast else pd.DataFrame()

st.title("⚡ European Electricity Market Explorer")
all_zones = list(ZONE_NAMES.keys())
selected_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]
plot_df = pd.DataFrame()
full_price_df = pd.DataFrame()
gen_df = pd.DataFrame()
forecast_df = pd.DataFrame() 

if len(d_range) == 2:
    full_price_df = fetch_data(all_zones, d_range[0], d_range[1])
    gen_df = fetch_gen_data(selected_codes, d_range[0], d_range[1])
    
    if selected_gen_types:
        forecast_df_raw = fetch_forecast_data(selected_codes, d_range[0], d_range[1])
    else:
        forecast_df_raw = pd.DataFrame()
        
    if not full_price_df.empty:
        freq = '60min' if res == "60 min" else '15min'
        full_price_resampled = full_price_df.groupby('Zone').apply(
            lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()
        ).reset_index()
        
        plot_df = full_price_resampled[full_price_resampled['Zone'].isin(selected_codes)].copy()
        plot_df['Currency'] = plot_df['Zone'].apply(lambda x: ZONE_NAMES.get(x, ['', 'EUR'])[1])
        plot_df['Display'] = plot_df['Zone'].apply(lambda x: f"{x} ({ZONE_NAMES.get(x, ['', 'EUR'])[1]}/MWh)")

        if not forecast_df_raw.empty:
            forecast_df = forecast_df_raw.groupby('Zone').apply(
                lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()
            ).reset_index()

col_chart, col_map = st.columns([2, 1])
with col_chart:
    st.subheader("Day-Ahead Prices and Generation Forecasts")
    if not plot_df.empty: 
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        for zone in selected_codes:
            zone_df = plot_df[plot_df['Zone'] == zone]
            currency = ZONE_NAMES[zone][1]
            fig.add_trace(
                go.Scatter(x=zone_df['Time'], y=zone_df['Price'], 
                           name=f"{zone} Price ({currency}/MWh)",
                           hovertemplate="%{y:.2f}",
                           hoverlabel=dict(namelength=-1),
                           line=dict(width=2)),
                secondary_y=False
            )
        if selected_gen_types and not forecast_df.empty:
            for zone in selected_codes:
                z_gen_df = forecast_df[forecast_df['Zone'] == zone]
                if not z_gen_df.empty:
                    for g_type in selected_gen_types:
                        if g_type in z_gen_df.columns:
                            fig.add_trace(
                                go.Scatter(x=z_gen_df['Time'], y=z_gen_df[g_type], 
                                           name=f"{zone} {g_type} Forecast (MW)",
                                           hovertemplate="%{y:.0f}",
                                           hoverlabel=dict(namelength=-1),
                                           line=dict(dash='dot', width=1)),
                                secondary_y=True
                            )

        p_min, p_max = plot_df['Price'].min(), plot_df['Price'].max()
        p_padding = (p_max - p_min) * 0.1 if p_max != p_min else 10
        p_range = [p_min - p_padding, p_max + p_padding]
        
        if not forecast_df.empty and selected_gen_types:
            available_gens = [g for g in selected_gen_types if g in forecast_df.columns]
            if available_gens:
                g_max = forecast_df[available_gens].max().max()
                g_max = g_max * 1.1 if g_max > 0 else 100
                g_min = (p_range[0] / p_range[1]) * g_max if p_range[1] > 0 else 0
                g_range = [g_min, g_max]
            else:
                g_range = [0, 100]
        else:
            g_range = [0, 100]

        fig.update_layout(
            template="plotly_white",
            hovermode="x unified",
            hoverlabel=dict(bgcolor="black", font_size=12, font_color="white", font_family="Arial"),
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=0, r=0, b=0, t=20)
        )
        fig.update_yaxes(title_text="Price", secondary_y=False, range=p_range, zeroline=True, zerolinewidth=2, zerolinecolor='rgba(0,0,0,0.3)')
        fig.update_yaxes(title_text="Generation Forecast [MW]", secondary_y=True, range=g_range, showgrid=False, zeroline=True, zerolinewidth=2, zerolinecolor='rgba(0,0,0,0.3)')
        st.plotly_chart(fig, use_container_width=True)

with col_map:
    def load_and_get_centers(folder_path):
        combined = {"type": "FeatureCollection", "features": []}
        centers = []
        found_zones = []
        files = glob.glob(os.path.join(folder_path, "*.geojson")) + glob.glob(os.path.join(folder_path, "*.txt"))
        for file in files:
            try:
                with open(file, "r") as f:
                    data = json.load(f)
                    features = data["features"] if "features" in data else [data]
                    for feature in features:
                        combined["features"].append(feature)
                        z_name = feature["properties"]["zoneName"]
                        found_zones.append(z_name)
                        geom = feature["geometry"]
                        if geom["type"] == "Polygon": coords = np.array(geom["coordinates"][0])
                        elif geom["type"] == "MultiPolygon": coords = np.array(max(geom["coordinates"], key=lambda x: len(x[0]))[0])
                        if len(coords) > 0:
                            min_lon, min_lat = np.min(coords, axis=0)
                            max_lon, max_lat = np.max(coords, axis=0)
                            centers.append({"Zone": z_name, "lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2})
            except: continue
        return combined, pd.DataFrame(centers), found_zones

    geojson_folder = "geojson_files"
    if os.path.exists(geojson_folder):
        geojson_data, centers_df, all_found_codes = load_and_get_centers(geojson_folder)
        if geojson_data["features"]:
            avg_prices = full_price_resampled.groupby('Zone')['Price'].mean().to_dict() if not full_price_resampled.empty else {}
            map_rows = []
            for k in all_found_codes:
                price = avg_prices.get(k, None)
                currency = ZONE_NAMES.get(k, ["", "EUR"])[1]
                map_rows.append({"Zone": k, "Selected": 1 if k in selected_codes else 0, "AvgPrice": f"{price:.2f}" if price is not None else "N/A", "Currency": currency})
            map_df = pd.DataFrame(map_rows)
            fig_map = px.choropleth(map_df, geojson=geojson_data, locations="Zone", featureidkey="properties.zoneName", color="Selected", color_continuous_scale=["#262730", "#007927"], custom_data=["AvgPrice", "Currency"])
            fig_map.update_traces(hovertemplate="<b>Zone:</b> %{location}<br><b>Baseload Price:</b> %{customdata[0]} %{customdata[1]}/MWh<extra></extra>")
            if not centers_df.empty:
                fig_map.add_scattergeo(lat=centers_df['lat'], lon=centers_df['lon'], text=centers_df['Zone'], mode='text', textfont=dict(size=10, color="#FFFFFF", family="Arial Black"), showlegend=False, hoverinfo="skip")
            fig_map.update_geos(center=dict(lon=12, lat=52), projection_scale=7, visible=True, showcountries=True, countrycolor="#262730", lakecolor="white", landcolor="#e0e0e0", projection_type="mercator", bgcolor="rgba(0,0,0,0)")
            fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500, coloraxis_showscale=False, paper_bgcolor="rgba(0,0,0,0)", autosize=True)
            
            # --- UPDATED INTERACTIVE MAP LOGIC ---
            map_event = st.plotly_chart(fig_map, use_container_width=True, config={'displaylogo': False}, on_select="rerun", selection_mode="points")
            
            if map_event and "selection" in map_event and map_event["selection"]["points"]:
                clicked_code = map_event["selection"]["points"][0]["location"]
                clicked_label = f"{ZONE_NAMES[clicked_code][0]} ({clicked_code})"
                
                # Copy current selection from session state
                current_selection = list(st.session_state.selected_zones)
                
                # Toggle logic: if in list, remove it; if not, add it
                if clicked_label in current_selection:
                    current_selection.remove(clicked_label)
                else:
                    current_selection.append(clicked_label)
                
                # Update session state and force rerun to refresh Sidebar Multiselect
                st.session_state.selected_zones = current_selection
                st.rerun()

st.divider()
col_met, col_tab = st.columns([1, 2])

with col_met:
    st.subheader("Key Metrics")
    if not plot_df.empty:
        key_metrics_list = []
        for code in selected_codes:
            z_df = plot_df[plot_df['Zone'] == code].set_index('Time')
            neg_hours = len(z_df[z_df['Price'] < 0])
            min_price = z_df['Price'].min()
            min_time_ts = z_df['Price'].idxmin()
            time_format = "%d-%m-%y %H:%M"
            min_time_str = min_time_ts.strftime(time_format)
            currency = ZONE_NAMES.get(code, ["", "EUR"])[1]
            key_metrics_list.append({
                "Zone": code, 
                "Number of Negative Periods": neg_hours,
                "Lowest Price": f"{min_price:.2f} {currency}/MWh",
                "Lowest Price Date & Time": min_time_str
            })
        st.table(pd.DataFrame(key_metrics_list))
    else:
        st.info("Select zones to view key metrics.")
            
    st.subheader("Baseload & Capture Metrics")
    if not plot_df.empty and not gen_df.empty:
        freq = '60min' if res == "60 min" else '15min'
        gen_resampled = gen_df.groupby('Zone').apply(
            lambda x: x.set_index('Time').resample(freq).sum(numeric_only=True)
        ).reset_index()
    
        metrics_list = []
        for code in selected_codes:
            p_sub = plot_df[plot_df['Zone'] == code]
            g_sub = gen_resampled[gen_resampled['Zone'] == code]
            if exclude_neg:
                p_sub['Price'] = p_sub['Price'].clip(lower=0)
            m_df = pd.merge(p_sub, g_sub, on='Time', how='inner')
            baseload = p_sub['Price'].mean()
            currency = ZONE_NAMES[code][1]
            sol_cap = "N/A"
            if 'Solar' in m_df.columns:
                total_sol = m_df['Solar'].sum()
                if total_sol > 0:
                    sol_cap = f"{(m_df['Price'] * m_df['Solar']).sum() / total_sol:.2f}"
            onshore_cap = "N/A"
            if 'Wind Onshore' in m_df.columns:
                total_onshore = m_df['Wind Onshore'].sum()
                if total_onshore > 0:
                    onshore_cap = f"{(m_df['Price'] * m_df['Wind Onshore']).sum() / total_onshore:.2f}"
            offshore_cap = "N/A"
            if 'Wind Offshore' in m_df.columns:
                total_offshore = m_df['Wind Offshore'].sum()
                if total_offshore > 0:
                    offshore_cap = f"{(m_df['Price'] * m_df['Wind Offshore']).sum() / total_offshore:.2f}"
            metrics_list.append({
                "Zone": code, "Baseload": f"{baseload:.2f}", 
                "Solar Capture": sol_cap, 
                "Wind Onshore": onshore_cap,
                "Wind Offshore": offshore_cap, 
                "Unit": f"{currency}/MWh"
            })
        st.table(pd.DataFrame(metrics_list))
    else:
        st.info("Select zones to calculate Capture prices.")

with col_tab:
    st.subheader("Data Table")
    if not plot_df.empty:
        table_df = plot_df.copy()
        if not forecast_df.empty:
            for g_type in selected_gen_types:
                if g_type in forecast_df.columns:
                    f_sub = forecast_df[['Time', 'Zone', g_type]].copy()
                    f_sub.columns = ['Time', 'Zone', f'TEMP_GEN_COL']
                    table_df = pd.merge(table_df, f_sub, on=['Time', 'Zone'], how='left')
                    table_df = table_df.rename(columns={'TEMP_GEN_COL': f"{g_type} Forecast (MW)"})
        table_df['Date'] = table_df['Time'].dt.strftime('%d-%m-%Y')
        table_df['24h Time'] = table_df['Time'].dt.strftime('%H:%M')
        val_cols = ['Price'] + [c for c in table_df.columns if 'Forecast (MW)' in c]
        pivot_base = table_df.melt(id_vars=['Date', '24h Time', 'Zone'], value_vars=val_cols)
        def get_header(row):
            if row['variable'] == 'Price':
                return f"{row['Zone']} ({ZONE_NAMES.get(row['Zone'], ['', 'EUR'])[1]}/MWh)"
            else:
                return f"{row['Zone']} {row['variable']}"
        pivot_base['Header'] = pivot_base.apply(get_header, axis=1)
        final_pivot = pivot_base.pivot_table(index=['Date', '24h Time'], columns='Header', values='value')
        st.dataframe(final_pivot.style.format("{:.2f}", na_rep="-"), use_container_width=True, height=400)

import streamlit as st

import pandas as pd

import plotly.express as px

import os

import json

import glob

import numpy as np

import requests

from datetime import datetime, timedelta

from entsoe import EntsoePandasClient

import plotly.graph_objects as go

from plotly.subplots import make_subplots

from concurrent.futures import ThreadPoolExecutor, as_completed

# NEW WEATHER INTEGRATION

import openmeteo_requests

import requests_cache

from retry_requests import retry



# 1. Config & API Setup

API_KEY = os.environ.get('ENTSOE_TOKEN')

client = EntsoePandasClient(api_key=API_KEY)



# Setup the Open-Meteo API client with cache

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)

retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

open_meteo_client = openmeteo_requests.Client(session=retry_session)



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

    "HR": ["Croatia", "EUR"], "HU": ["Hungary", "EUR"], 

    "ME": ["Montenegro", "EUR",], "MK": ["North Macedonia", "EUR"],

    "RO": ["Romania", "EUR"], "RS": ["Serbia", "EUR"], 

    "SI": ["Slovenia", "EUR"], "SK": ["Slovakia", "EUR"],

    "IT_NORD": ["Italy North", "EUR"], "IT_CNOR": ["Italy Central North", "EUR"],

    "IT_CSUD": ["Italy Central South", "EUR"], "IT_SUD": ["Italy South", "EUR"],

    "IT_SICI": ["Italy Sicily", "EUR"], "IT_SARD": ["Italy Sardinia", "EUR"], "IT_CALA": ["Italy Calabria", "EUR"]

}



# Coordinate Mapping for Weather Data

ZONE_COORDS = {

    "AT": [47.51, 14.55], "BE": [50.85, 4.35], "BG": [42.73, 25.48], "CH": [46.81, 8.22],

    "CZ": [49.81, 15.47], "DE_LU": [51.16, 10.45], "FR": [46.22, 2.21], "GB": [55.37, -3.43],

    "IE_SEM": [53.14, -7.69], "NL": [52.13, 5.29], "PL": [51.91, 19.14], "DK_1": [56.26, 9.50],

    "DK_2": [55.67, 12.00], "EE": [58.59, 25.01], "FI": [61.92, 25.74], "LT": [55.16, 23.88],

    "LV": [56.87, 24.60], "NO_1": [59.91, 10.75], "NO_2": [58.15, 8.00], "NO_3": [63.43, 10.39],

    "NO_4": [67.28, 14.40], "NO_5": [60.39, 5.32], "SE_1": [65.58, 22.15], "SE_2": [62.39, 17.30],

    "SE_3": [59.32, 18.06], "SE_4": [55.60, 13.00], "ES": [40.46, -3.74], "PT": [39.39, -8.22],

    "HR": [45.10, 15.20], "HU": [47.16, 19.50], "ME": [42.70, 19.37], "MK": [41.60, 21.74],

    "RO": [45.94, 24.96], "RS": [44.01, 21.00], "SI": [46.15, 14.99], "SK": [48.66, 19.69],

    "IT_NORD": [45.46, 9.19], "IT_CNOR": [43.76, 11.25], "IT_CSUD": [41.87, 12.49],

    "IT_SUD": [40.85, 14.26], "IT_SICI": [37.59, 14.01], "IT_SARD": [40.12, 9.01], "IT_CALA": [38.90, 16.58]

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

    

    st.session_state.selected_zones = st.multiselect("Select bidding zones:", 

                   options=sorted(display_options.keys()), 

                   default=st.session_state.selected_zones)

    

    gen_options = ["Solar", "Wind Onshore", "Wind Offshore"]

    selected_gen_types = st.multiselect("Overlay Generation Forecast:", options=gen_options, key="gen_forecast_select")

    

    # NEW: Weather Overlays

    weather_options = ["Solar Radiation", "Wind Speed (100m)"]

    selected_weather_types = st.multiselect("Overlay Weather Data:", options=weather_options, key="weather_select")

    

    res = st.radio("Resolution", ["Monthly", "Daily", "60 min", "15 min"], horizontal=True, key="res_radio")

    

    today = datetime.now().date()

    default_d_range = st.session_state.get("date_range_input", (today - timedelta(days=2), today))

    d_range = st.date_input("Date Range", value=default_d_range, key="date_range_input")

    

    exclude_neg = st.checkbox("Hard floor at 0", help="Treats negative prices as 0 for capture price calculation", key="neg_price_check")

    no_settle_neg = st.checkbox("No settlement for negative", help="Treats generation as 0 when prices are negative", key="no_settle_check")



    st.divider()

    st.subheader("PPA Configuration")

    ppa_price = st.number_input("PPA Price (EUR/MWh)", value=0.0, step = 1.0, key="ppa_price_input")

    fixed_floating = st.checkbox("Fixed for Floating Price Structure", key="fixed_float_check")

    

    market_following = st.checkbox("Market Following with floor", key="mkt_follow_check")

    if market_following:

        floor_rate_eur = st.number_input("Floor Rate (EUR/MWh)", value=0.0, step=0.1, key="floor_eur_input")

        floor_rate_pct = st.number_input("Floor Rate (% of PPA Price)", value=0.0, step=1.0, key="floor_pct_input")

        

        if floor_rate_eur > 0 and floor_rate_pct > 0:

            st.error("⚠️ Please specify either a Floor Rate (EUR) OR a Floor Rate (%), not both.")

        elif floor_rate_eur == 0 and floor_rate_pct == 0:

            st.warning("Please enter a floor rate for Market Following.")
            

# --- WEATHER FETCHING ---

@st.cache_data(ttl=3600)

def fetch_weather_data(codes, start_date, end_date):

    if not codes: return pd.DataFrame()

    all_weather = []

    

    for code in codes:

        if code not in ZONE_COORDS: continue

        lat, lon = ZONE_COORDS[code]

        

        url = "https://api.open-meteo.com/v1/forecast"

        params = {

            "latitude": lat,

            "longitude": lon,

            "hourly": ["shortwave_radiation", "wind_speed_100m"],

            "start_date": start_date.strftime('%Y-%m-%d'),

            "end_date": end_date.strftime('%Y-%m-%d'),

            "timezone": "UTC"

        }

        

        try:

            responses = open_meteo_client.weather_api(url, params=params)

            response = responses[0]

            hourly = response.Hourly()

            

            num_periods = int((hourly.TimeEnd() - hourly.Time()) / hourly.Interval())

            

            data = {

                "Time": pd.date_range(

                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),

                    periods=num_periods,

                    freq=pd.Timedelta(seconds=hourly.Interval()),

                    inclusive="left"

                ),

                "Solar Radiation": hourly.Variables(0).ValuesAsNumpy(),

                "Wind Speed (100m)": hourly.Variables(1).ValuesAsNumpy(),

                "Zone": code

            }

            df = pd.DataFrame(data)

            df['Time'] = df['Time'].dt.tz_convert('Europe/Brussels')

            all_weather.append(df)

        except: continue

        

    return pd.concat(all_weather) if all_weather else pd.DataFrame()



# --- LOAD CSV DATA ---

@st.cache_data(ttl=300)

def load_local_csv():

    if os.path.exists('market_prices.csv'):

        df = pd.read_csv('market_prices.csv')

        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date

        return df.dropna(subset=['Date']) # Ensure we don't process garbage/header rows

    return pd.DataFrame()



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

            res_data = future.result()

            if res_data is not None: all_data.append(res_data)

            

    return pd.concat(all_data) if all_data else pd.DataFrame()



@st.cache_data(ttl=3600)

def fetch_gen_data(codes, start_date, end_date):

    if not codes: return pd.DataFrame()

    start = pd.Timestamp(start_date, tz='Europe/Brussels')

    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)



    def get_gen(code):

        try:

            df = client.query_generation(code, start=start, end=end)

            if isinstance(df, pd.Series): df = df.to_frame()

            if isinstance(df.columns, pd.MultiIndex):

                df.columns = df.columns.get_level_values(0)

            df = df.T.groupby(level=0).sum().T 

            df.index.name = 'Time'

            df = df.reset_index()

            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')

            df['Zone'] = code

            return df

        except: return None



    all_gen = []

    with ThreadPoolExecutor(max_workers=10) as executor:

        futures = [executor.submit(get_gen, code) for code in codes]

        for future in as_completed(futures):

            res_data = future.result()

            if res_data is not None: all_gen.append(res_data)

            

    return pd.concat(all_gen) if all_gen else pd.DataFrame()



@st.cache_data(ttl=3600)

def fetch_forecast_data(codes, start_date, end_date):

    if not codes: return pd.DataFrame()

    start = pd.Timestamp(start_date, tz='Europe/Brussels')

    end = pd.Timestamp(end_date, tz='Europe/Brussels') + pd.Timedelta(days=1)

    

    def get_forecast(code):

        try:

            df = client.query_wind_and_solar_forecast(code, start=start, end=end)

            if isinstance(df, pd.Series): df = df.to_frame()

            if isinstance(df.columns, pd.MultiIndex):

                df.columns = df.columns.get_level_values(0)

            df = df.T.groupby(level=0).sum().T 

            df.index.name = 'Time'

            df = df.reset_index()

            df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Europe/Brussels')

            df['Zone'] = code

            return df

        except: return None



    all_forecast = []

    with ThreadPoolExecutor(max_workers=10) as executor:

        futures = [executor.submit(get_forecast, code) for code in codes]

        for future in as_completed(futures):

            res_data = future.result()

            if res_data is not None: all_forecast.append(res_data)

            

    return pd.concat(all_forecast) if all_forecast else pd.DataFrame()



st.title("⚡ European Electricity Market Explorer")

all_zones = list(ZONE_NAMES.keys())

selected_codes = [display_options[lbl] for lbl in st.session_state.selected_zones]

plot_df = pd.DataFrame()

full_price_df = pd.DataFrame()

gen_df = pd.DataFrame()

forecast_df = pd.DataFrame() 

forecast_df_raw = pd.DataFrame()

weather_df = pd.DataFrame()

weather_df_raw = pd.DataFrame() 



# --- DATA PROCESSING ---

if len(d_range) == 2:

    res_map = {"15 min": "15min", "60 min": "60min", "Daily": "D", "Monthly": "MS"}

    freq = res_map.get(res, "60min")



    if res in ["Daily", "Monthly"]:

        csv_raw = load_local_csv()

        if not csv_raw.empty:

            val_col = 'Price' if 'Price' in csv_raw.columns else ('Value' if 'Value' in csv_raw.columns else 'MW')

            mask = (csv_raw['Date'] >= d_range[0]) & (csv_raw['Date'] <= d_range[1])

            data_subset = csv_raw[mask].copy()

            

            full_price_df = data_subset[data_subset['Metric'] == 'Baseload'].copy()

            if not full_price_df.empty:

                full_price_df = full_price_df.rename(columns={'Date': 'Time', 'Country': 'Zone', val_col: 'Price'})

                full_price_df['Time'] = pd.to_datetime(full_price_df['Time']).dt.tz_localize('Europe/Brussels', ambiguous='infer')

            

            gen_subset = data_subset[data_subset['Metric'].str.contains(' Generation', na=False)].copy()

            if not gen_subset.empty:

                gen_subset['Metric'] = gen_subset['Metric'].str.replace(' Generation', '', regex=False).str.strip()

                gen_pivot = gen_subset.pivot_table(index=['Date', 'Country'], columns='Metric', values=val_col).reset_index()

                gen_pivot = gen_pivot.rename(columns={'Date': 'Time', 'Country': 'Zone'})

                gen_pivot['Time'] = pd.to_datetime(gen_pivot['Time']).dt.tz_localize('Europe/Brussels', ambiguous='infer')

                gen_df = gen_pivot[gen_pivot['Zone'].isin(selected_codes)]

                

            forecast_subset = data_subset[data_subset['Metric'].str.contains(' Forecast', na=False)].copy()

            if not forecast_subset.empty:

                forecast_subset['Metric'] = forecast_subset['Metric'].str.replace(' Forecast', '', regex=False).str.strip()

                fc_pivot = forecast_subset.pivot_table(index=['Date', 'Country'], columns='Metric', values=val_col).reset_index()

                fc_pivot = fc_pivot.rename(columns={'Date': 'Time', 'Country': 'Zone'})

                fc_pivot['Time'] = pd.to_datetime(fc_pivot['Time']).dt.tz_localize('Europe/Brussels', ambiguous='infer')

                forecast_df_raw = fc_pivot[fc_pivot['Zone'].isin(selected_codes)]

    else:

        full_price_df = fetch_data(all_zones, d_range[0], d_range[1])

        gen_df = fetch_gen_data(selected_codes, d_range[0], d_range[1])

        if selected_gen_types:

            forecast_df_raw = fetch_forecast_data(selected_codes, d_range[0], d_range[1])



    # FETCH WEATHER

    if selected_weather_types:

        weather_df_raw = fetch_weather_data(selected_codes, d_range[0], d_range[1])



    # RESAMPLE EVERYTHING CONSISTENTLY

    if not full_price_df.empty:

        full_price_resampled = full_price_df.groupby('Zone').apply(

            lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()

        ).reset_index()

        plot_df = full_price_resampled[full_price_resampled['Zone'].isin(selected_codes)].copy()

        # FIX: Filter out timezone-induced extra days

        plot_df = plot_df[plot_df['Time'].dt.date <= d_range[1]]

        

    if not forecast_df_raw.empty:

        forecast_df = forecast_df_raw.groupby('Zone').apply(

            lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()

        ).reset_index()

        # FIX: Filter out timezone-induced extra days

        forecast_df = forecast_df[forecast_df['Time'].dt.date <= d_range[1]]

            

    if selected_weather_types and not weather_df_raw.empty:

        weather_df = weather_df_raw.groupby('Zone').apply(

            lambda x: x.set_index('Time').resample(freq).mean(numeric_only=True).ffill()

        ).reset_index()

        # FIX: Filter out timezone-induced extra days

        weather_df = weather_df[weather_df['Time'].dt.date <= d_range[1]]



col_chart, col_map = st.columns([2, 1])

with col_chart:



    st.subheader("Day-Ahead Prices, Generation Forecasts & Weather Data")



    



    # 1. Determine active rows dynamically



    active_rows = []



    if not plot_df.empty: active_rows.append("Prices")



    if selected_gen_types and not forecast_df.empty: active_rows.append("Generation")



    if selected_weather_types and not weather_df.empty: active_rows.append("Weather")



    



    n_rows = len(active_rows)



    



    if n_rows > 0:



        # Create mapping for row indices



        row_map_idx = {name: i + 1 for i, name in enumerate(active_rows)}



        



        fig = make_subplots(



            rows=n_rows, cols=1, 



            shared_xaxes=True, 



            vertical_spacing=0.07,



            subplot_titles=[f"Market {name}" if name == "Prices" else f"{name} Data" for name in active_rows]



        )



        



        colors = px.colors.qualitative.Plotly



        zone_color_map = {zone: colors[i % len(colors)] for i, zone in enumerate(selected_codes)}



        



# 1. Prices
        if "Prices" in row_map_idx:
            curr_row = row_map_idx["Prices"]
            for zone in selected_codes:
                zone_df = plot_df[plot_df['Zone'] == zone]
                if not zone_df.empty:
                    currency = ZONE_NAMES[zone][1]
                    fig.add_trace(go.Scatter(
                        x=zone_df['Time'], 
                        y=zone_df['Price'], 
                        name=f"{zone} Price", 
                        line=dict(color=zone_color_map[zone], width=2),
                        # Customdata isn't strictly needed if we use 'x unified' correctly with shared axes
                        hovertemplate="Price: %{y:.2f} " + currency + "/MWh<extra></extra>"
                    ), row=curr_row, col=1)
            
            if ppa_price > 0:
                fig.add_trace(go.Scatter(
                    x=plot_df['Time'].unique(), 
                    y=[ppa_price]*len(plot_df['Time'].unique()), 
                    name="PPA Price", 
                    line=dict(color='red', dash='dash', width=2), 
                    hovertemplate="PPA: %{y:.2f}<extra></extra>"
                ), row=curr_row, col=1)

        # 2. Generation
        if "Generation" in row_map_idx:
            curr_row = row_map_idx["Generation"]
            for zone in selected_codes:
                z_gen_df = forecast_df[forecast_df['Zone'] == zone]
                if not z_gen_df.empty:
                    for g_type in selected_gen_types:
                        if g_type in z_gen_df.columns:
                            fig.add_trace(go.Scatter(
                                x=z_gen_df['Time'], 
                                y=z_gen_df[g_type], 
                                name=f"{zone} {g_type}", 
                                line=dict(color=zone_color_map[zone], dash='dot', width=1), 
                                hovertemplate=f"{g_type}: %{{y:.2f}} MW<extra></extra>"
                            ), row=curr_row, col=1)

        # 3. Weather
        if "Weather" in row_map_idx:
            curr_row = row_map_idx["Weather"]
            for zone in selected_codes:
                z_weather_df = weather_df[weather_df['Zone'] == zone]
                if not z_weather_df.empty:
                    for w_type in selected_weather_types:
                        if w_type in z_weather_df.columns:
                            unit = "W/m²" if w_type == "Solar Radiation" else "m/s"
                            fig.add_trace(go.Scatter(
                                x=z_weather_df['Time'], 
                                y=z_weather_df[w_type], 
                                name=f"{zone} {w_type}", 
                                line=dict(color=zone_color_map[zone], dash='dashdot', width=1.5), 
                                opacity=0.8, 
                                hovertemplate=f"{w_type}: %{{y:.2f}} {unit}<extra></extra>"
                            ), row=curr_row, col=1)

        # Dynamic height calculation
        dynamic_height = 300 + (n_rows * 170)

        fig.update_layout(
            height=dynamic_height,
            template="plotly_white", 
            hovermode="x unified", 
            hoverdistance=-1,
            spikedistance=-1,
            legend=dict(orientation="h", y=-0.15 if n_rows > 1 else -0.3), 
            margin=dict(l=0, r=0, b=0, t=40)
        )

        # 1. Merge all hover data onto the first X-axis
        fig.update_traces(xaxis="x")

        # 2. Re-link every Y-axis to that single X-axis so the charts stay in their rows
        for i in range(1, n_rows + 1):
            y_axis_key = f"yaxis{i if i > 1 else ''}"
            if y_axis_key in fig.layout:
                fig.layout[y_axis_key]["anchor"] = "x"

        # 3. CONFIGURE THE SHARED AXIS ('xaxis')
        # We target 'xaxis' specifically because that's where all the traces now live
        fig.update_layout(
            xaxis=dict(
                showticklabels=True,
                side="bottom",      # Forces the labels to appear at the bottom of the last chart
                showspikes=True,
                spikemode='across',
                spikesnap='cursor',
                spikethickness=1,
                spikecolor="#999999",
                spikedash="dot",
                type='date'         # Ensures it handles time data correctly
            )
        )

        # 4. Remove any loops that hide 'row=1' labels, as 'row=1' is now our primary axis
        
        st.plotly_chart(fig, use_container_width=True)



with col_map:

    def load_and_get_centers(folder_path):

        combined, centers, found_zones = {"type": "FeatureCollection", "features": []}, [], []

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

                        coords = np.array(geom["coordinates"][0]) if geom["type"] == "Polygon" else np.array(max(geom["coordinates"], key=lambda x: len(x[0]))[0])

                        min_lon, min_lat = np.min(coords, axis=0)

                        max_lon, max_lat = np.max(coords, axis=0)

                        centers.append({"Zone": z_name, "lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2})

            except: continue

        return combined, pd.DataFrame(centers), found_zones



    geojson_folder = "geojson_files"

    if os.path.exists(geojson_folder):

        geojson_data, centers_df, all_found_codes = load_and_get_centers(geojson_folder)

        if geojson_data["features"]:

            avg_prices = plot_df.groupby('Zone')['Price'].mean().to_dict() if not plot_df.empty else {}

            map_df = pd.DataFrame([{"Zone": k, "Selected": 1 if k in selected_codes else 0, "AvgPrice": f"{avg_prices.get(k, 0):.2f}", "Currency": ZONE_NAMES.get(k, ["", "EUR"])[1]} for k in all_found_codes])

            

            fig_map = px.choropleth(map_df, geojson=geojson_data, locations="Zone", featureidkey="properties.zoneName", color="Selected", color_continuous_scale=["#262730", "#007927"], custom_data=["AvgPrice", "Currency"])

            

            if not centers_df.empty:

                label_df = centers_df[centers_df['Zone'].isin(all_found_codes)]

                fig_map.add_trace(go.Scattergeo(

                    lon=label_df['lon'],

                    lat=label_df['lat'],

                    text=label_df['Zone'],

                    mode='text',

                    textfont=dict(size=10, color="white"),

                    showlegend=False,

                    hoverinfo='skip'

                ))



            fig_map.update_geos(center=dict(lon=12, lat=52), projection_scale=7, projection_type="mercator", bgcolor="rgba(0,0,0,0)")

            fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500, coloraxis_showscale=False, paper_bgcolor="rgba(0,0,0,0)")

            

            map_event = st.plotly_chart(fig_map, width='stretch', on_select="rerun", selection_mode="points")

            if map_event and "selection" in map_event and map_event["selection"]["points"]:

                clicked_code = map_event["selection"]["points"][0].get("location")

                if clicked_code:

                    clicked_label = f"{ZONE_NAMES[clicked_code][0]} ({clicked_code})"

                    curr = list(st.session_state.selected_zones)

                    if clicked_label in curr: curr.remove(clicked_label)

                    else: curr.append(clicked_label)

                    st.session_state.selected_zones = curr

                    st.rerun()



st.divider()

col_met, col_tab = st.columns([1, 2])

with col_met:

    st.subheader("Key Metrics")

    if not full_price_df.empty:

        metrics = []

        for code in selected_codes:

            z_raw = full_price_df[full_price_df['Zone'] == code]

            metrics.append({

                "Zone": code, 

                "Negative Periods": len(z_raw[z_raw['Price'] < 0]), 

                "Lowest Price": f"{z_raw['Price'].min():.2f} {ZONE_NAMES[code][1]}/MWh"

            })

        st.table(pd.DataFrame(metrics))



    st.subheader("Baseload & Capture Metrics")

    if not full_price_df.empty and not gen_df.empty:

        p_raw = full_price_df[full_price_df['Zone'].isin(selected_codes)].copy()

        p_raw['is_negative_hour'] = p_raw['Price'] < 0

        

        if exclude_neg:

            p_raw['Price'] = p_raw['Price'].clip(lower=0)

            

        m_df_raw = pd.merge(p_raw, gen_df, on=['Time', 'Zone'], how='inner')

        

        if no_settle_neg:

            for col in ['Solar', 'Wind Onshore', 'Wind Offshore']:

                if col in m_df_raw.columns:

                    m_df_raw.loc[m_df_raw['is_negative_hour'], col] = 0

    

        metrics_list = []

        for code in selected_codes:

            zone_m = m_df_raw[m_df_raw['Zone'] == code]

            baseload = p_raw[p_raw['Zone'] == code]['Price'].mean()

            currency = ZONE_NAMES[code][1]

            

            sol_cap = "N/A"

            if 'Solar' in zone_m.columns:

                total_sol = zone_m['Solar'].sum()

                if total_sol > 0:

                    sol_cap = f"{(zone_m['Price'] * zone_m['Solar']).sum() / total_sol:.2f}"

            

            onshore_cap = "N/A"

            if 'Wind Onshore' in zone_m.columns:

                total_onshore = zone_m['Wind Onshore'].sum()

                if total_onshore > 0:

                    onshore_cap = f"{(zone_m['Price'] * zone_m['Wind Onshore']).sum() / total_onshore:.2f}"

            

            offshore_cap = "N/A"

            if 'Wind Offshore' in zone_m.columns:

                total_offshore = zone_m['Wind Offshore'].sum()

                if total_offshore > 0:

                    offshore_cap = f"{(zone_m['Price'] * zone_m['Wind Offshore']).sum() / total_offshore:.2f}"

            

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

    if not plot_df.empty or not weather_df.empty:

        # Use price as base if available, else weather

        table_df = plot_df.copy() if not plot_df.empty else weather_df[['Time', 'Zone']].copy()

        

        if not forecast_df.empty:

            table_df = table_df.merge(forecast_df, on=['Time', 'Zone'], how='outer')

        

        if not weather_df.empty:

            table_df = table_df.merge(weather_df, on=['Time', 'Zone'], how='outer')



        if fixed_floating and ppa_price > 0 and 'Price' in table_df.columns:

            table_df['Fixed for Floating settlement'] = table_df['Price'] - ppa_price

        if market_following and ppa_price > 0 and 'Price' in table_df.columns:

            eff_floor = floor_rate_eur if floor_rate_eur > 0 else (floor_rate_pct / 100 * ppa_price)

            diff = table_df['Price'] - ppa_price

            table_df['Market following settlement'] = np.where(table_df['Price'] > ppa_price, np.minimum(diff, eff_floor), diff)

        

        table_df['Date'] = table_df['Time'].dt.strftime('%d-%m-%Y')

        table_df['24h Time'] = table_df['Time'].dt.strftime('%H:%M')

        

        gen_cols = [c for c in forecast_df.columns if c in selected_gen_types]

        weather_cols = [c for c in weather_df.columns if c in selected_weather_types]

        

        value_vars = []

        if 'Price' in table_df.columns: value_vars.append('Price')

        value_vars += gen_cols + weather_cols

        

        if 'Fixed for Floating settlement' in table_df.columns: value_vars.append('Fixed for Floating settlement')

        if 'Market following settlement' in table_df.columns: value_vars.append('Market following settlement')



        wide_df = table_df.pivot(index=['Time', 'Date', '24h Time'], columns='Zone', values=value_vars)



        new_columns = []

        for col in wide_df.columns:

            metric, zone = col

            currency = ZONE_NAMES[zone][1]

            if metric == 'Price':

                new_columns.append(f"{zone} Price ({currency}/MWh)")

            elif metric in gen_cols:

                new_columns.append(f"{zone} {metric} (MW)")

            elif metric in weather_cols:

                unit = "W/m²" if metric == "Solar Radiation" else "m/s"

                new_columns.append(f"{zone} {metric} ({unit})")

            else:

                new_columns.append(f"{zone} {metric}")

        

        wide_df.columns = new_columns

        wide_df = wide_df.reset_index().sort_values('Time')



        cols_to_show = ['Date', '24h Time'] + [c for c in wide_df.columns if c not in ['Time', 'Date', '24h Time']]

            

        st.dataframe(

            wide_df[cols_to_show].style.format(precision=2, na_rep="-"), 

            width='stretch', 

            height=400,

            hide_index=True

        )

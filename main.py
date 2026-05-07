import os
import pandas as pd
from datetime import datetime
from entsoe import EntsoePandasClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. Config
ENTSOE_API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=ENTSOE_API_KEY)

# --- TOGGLE THIS VALUE ---
DAYS_TO_FETCH = 1825 # Set to 1825 for 5-year heavy lift, 10 for daily updates
# -------------------------

countries = [
    'AT', 'BE', 'CH', 'CZ', 'DE_LU', 'FR', 'GB', 'IE_SEM', 'NL', 'PL',
    'DK_1', 'DK_2', 'EE', 'FI', 'LT', 'LV', 'NO_1', 'NO_2', 'NO_3', 
    'NO_4', 'NO_5', 'SE_1', 'SE_2', 'SE_3', 'SE_4', 'BG', 'ES', 'GR', 
    'HR', 'HU', 'ME', 'MK', 'PT', 'RO', 'RS', 'SI', 'SK', 'IT_NORD', 
    'IT_CNOR', 'IT_CSUD', 'IT_SUD', 'IT_SICI', 'IT_SARD', 'IT_CALA'
]

csv_filename = 'market_prices.csv'

def process_metrics(price_series, gen_df, forecast_df, country_code):
    if price_series is None or price_series.empty: 
        return pd.DataFrame()
    
    price_series.index = pd.to_datetime(price_series.index)
    data = []
    
    # 1. Baseload Price
    baseload = price_series.resample('D').mean()
    for date, val in baseload.items():
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Baseload', 'Price': val})

    mapping = {
        'Solar': 'Solar',
        'Wind Onshore': 'Wind Onshore',
        'Wind Offshore': 'Wind Offshore'
    }

    # 2. Process Actual Generation & Capture Prices
    if gen_df is not None and not gen_df.empty:
        if isinstance(gen_df.columns, pd.MultiIndex):
            gen_df.columns = gen_df.columns.get_level_values(0)
        
        gen_df = gen_df.T.groupby(level=0).sum().T
        combined = pd.merge(price_series.to_frame('Price'), gen_df, left_index=True, right_index=True, how='inner')
        
        for fuel, label in mapping.items():
            if fuel in combined.columns:
                daily_gen = combined[fuel].resample('D').sum()
                for date, val in daily_gen.items():
                    if pd.notna(val): data.append({'Date': date.date(), 'Metric': f'{label} Generation', 'Price': val})
                
                def calc_cap(group):
                    total_mwh = group[fuel].sum()
                    return (group['Price'] * group[fuel]).sum() / total_mwh if total_mwh > 0 else None
                
                cap_series = combined.resample('D').apply(calc_cap)
                for date, val in cap_series.items():
                    if pd.notna(val): data.append({'Date': date.date(), 'Metric': f'{label} Capture', 'Price': val})

    # 3. Process Forecasts
    if forecast_df is not None and not forecast_df.empty:
        if isinstance(forecast_df.columns, pd.MultiIndex):
            forecast_df.columns = forecast_df.columns.get_level_values(0)
        
        forecast_df = forecast_df.T.groupby(level=0).sum().T
        for fuel, label in mapping.items():
            if fuel in forecast_df.columns:
                daily_fc = forecast_df[fuel].resample('D').sum()
                for date, val in daily_fc.items():
                    if pd.notna(val): data.append({'Date': date.date(), 'Metric': f'{label} Forecast', 'Price': val})

    res = pd.DataFrame(data)
    res['Country'] = country_code
    return res

def fetch_single_country(code, start_date, end_date):
    try:
        prices = client.query_day_ahead_prices(code, start=start_date, end=end_date)
        try:
            gen = client.query_generation(code, start=start_date, end=end_date)
        except:
            gen = None
        try:
            forecast = client.query_wind_and_solar_forecast(code, start=start_date, end=end_date)
        except:
            forecast = None
        return process_metrics(prices, gen, forecast, code)
    except Exception as e:
        print(f"Error {code}: {str(e)[:50]}")
        return pd.DataFrame()

# --- Chunking Logic ---
chunks = []
current_end = pd.Timestamp(datetime.now(), tz='Europe/Brussels') + pd.Timedelta(days=1)
remaining = DAYS_TO_FETCH

while remaining > 0:
    step = min(remaining, 365)
    start_date = current_end - pd.Timedelta(days=step)
    chunks.append((start_date, current_end))
    current_end = start_date
    remaining -= step

# Execution Loop
# REMOVED: The os.remove() block that was deleting your historical data.

for start_date, end_date in chunks:
    print(f"\nProcessing window: {start_date.date()} to {end_date.date()}")
    all_country_data = []
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_single_country, code, start_date, end_date): code for code in countries}
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if not result.empty:
                all_country_data.append(result)
            print(f"Progress: {i+1}/{len(countries)} zones finished.")

    # We still append chunk-by-chunk to keep memory low and prevent crash-loss during the 5-year run
    if all_country_data:
        final_df = pd.concat(all_country_data, ignore_index=True)
        final_df['Price'] = final_df['Price'].round(2)
        file_exists = os.path.isfile(csv_filename)
        final_df.to_csv(csv_filename, mode='a', index=False, header=not file_exists)

# NEW: Cleanup and Deduplication Phase
if os.path.exists(csv_filename):
    print("\nCleaning up overlapping dates to prevent CSV bloat...")
    # Read the full file
    full_df = pd.read_csv(csv_filename)
    # Drop duplicates based on Date, Metric, and Country, keeping the most recently fetched row ('last')
    full_df = full_df.drop_duplicates(subset=['Date', 'Metric', 'Country'], keep='last')
    # Save it cleanly back over the file
    full_df.to_csv(csv_filename, index=False)

print(f"\n✅ Success: Data saved and deduplicated in {csv_filename}")

import os
import time
import pandas as pd
import io
import requests
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Config
ENTSOE_API_KEY = os.environ.get('ENTSOE_TOKEN')
ELEXON_TOKEN = os.environ.get('ELEXON_TOKEN') 

client = EntsoePandasClient(api_key=ENTSOE_API_KEY)

countries = [
    'AT', 'BE', 'BG', 'CH', 'CZ', 'DE', 'DK_1', 'DK_2', 'EE', 'ES', 'FI', 
    'FR', 'GB', 'GR', 'HR', 'HU', 'IE_SEM', 'IT_NORD', 'IT_CNOR', 'IT_CSUD', 
    'IT_SUD', 'IT_SICI', 'IT_SARD', 'LT', 'LU', 'LV', 'NL', 'NO_1', 'NO_2', 
    'NO_3', 'NO_4', 'NO_5', 'PL', 'PT', 'RO', 'RS', 'SE_1', 'SE_2', 'SE_3', 
    'SE_4', 'SI', 'SK'
]

end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
start = end - pd.Timedelta(days=10)

def process_to_long_format(price_series, country_code):
    if price_series is None or price_series.empty: 
        return pd.DataFrame()
    
    price_series.index = pd.to_datetime(price_series.index)
    price_series = price_series[price_series.notna()]
    
    # We allow 0 here because occasionally prices ARE zero, 
    # but we filter out rows where data is clearly missing.
    baseload = price_series.resample('D').mean()
    peak = price_series.between_time('08:00', '19:59').resample('D').mean()
    off_peak_mask = ~price_series.index.isin(price_series.between_time('08:00', '19:59').index)
    off_peak = price_series.loc[off_peak_mask].resample('D').mean()
    
    data = []
    for date, val in baseload.items(): 
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Baseload', 'Price': val})
    for date, val in peak.items(): 
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Peak', 'Price': val})
    for date, val in off_peak.items(): 
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Off-Peak', 'Price': val})
    
    res = pd.DataFrame(data)
    res['Country'] = country_code
    return res

def fetch_gb_direct_csv(start_date, end_date):
    if not ELEXON_TOKEN: return None
    try:
        # SWITCHED to BESTVIEWPRICES_FILE for better data coverage
        url = f"https://downloads.elexonportal.co.uk/file/download/BESTVIEWPRICES_FILE?key={ELEXON_TOKEN}"
        r = requests.get(url, timeout=30)
        if r.status_code != 200: return None
        
        # Read the CSV - Elexon CSVs usually have a header row
        df = pd.read_csv(io.StringIO(r.text), skipinitialspace=True)
        df.columns = [c.strip().replace('"', '').replace("'", "") for c in df.columns]
        
        # Find columns flexibly
        date_col = next((c for c in df.columns if 'date' in c.lower()), df.columns[0])
        period_col = next((c for c in df.columns if 'period' in c.lower()), df.columns[1])
        # Look for "Best View Price" or any "Price" column
        price_col = next((c for c in df.columns if 'price' in c.lower()), df.columns[-1])

        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
        df[period_col] = pd.to_numeric(df[period_col], errors='coerce')
        
        df = df.dropna(subset=[date_col, price_col])
        
        # GB uses 30-min settlement periods (1-48)
        df['Time'] = df.apply(lambda x: x[date_col] + timedelta(minutes=30 * (int(x[period_col]) - 1)), axis=1)
        df = df.set_index('Time')[price_col].sort_index()
        
        # Filter for the requested time window
        return df[start_date.tz_localize(None) : end_date.tz_localize(None)]
    except Exception as e:
        print(f"GB Fetch Error: {e}")
        return None

all_country_data = []

for code in countries:
    try:
        print(f"Fetching {code}...")
        raw_series = fetch_gb_direct_csv(start, end) if code == 'GB' else client.query_day_ahead_prices(code, start=start, end=end)
            
        if raw_series is not None and not raw_series.empty:
            processed = process_to_long_format(raw_series, code)
            all_country_data.append(processed)
        
        time.sleep(1.1) 
    except Exception as e:
        print(f"Skipping {code}: {e}")

if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv('market_prices.csv', index=False)
    print("Success: market_prices.csv updated.")

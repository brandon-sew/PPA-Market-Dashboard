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
    
    baseload = price_series.resample('D').mean()
    peak = price_series.between_time('08:00', '19:59').resample('D').mean()
    off_peak_mask = ~price_series.index.isin(price_series.between_time('08:00', '19:59').index)
    off_peak = price_series.loc[off_peak_mask].resample('D').mean()
    
    data = []
    for date, val in baseload.items(): 
        data.append({'Date': date.date(), 'Metric': 'Baseload', 'Price': val})
    for date, val in peak.items(): 
        data.append({'Date': date.date(), 'Metric': 'Peak', 'Price': val})
    for date, val in off_peak.items(): 
        data.append({'Date': date.date(), 'Metric': 'Off-Peak', 'Price': val})
    
    res = pd.DataFrame(data)
    res['Country'] = country_code
    return res

def fetch_gb_direct_csv(start_date, end_date):
    if not ELEXON_TOKEN:
        print("GB Skip: ELEXON_TOKEN secret missing.")
        return None
        
    try:
        url = f"https://downloads.elexonportal.co.uk/file/download/LATEST_MID_FILE?key={ELEXON_TOKEN}"
        response = requests.get(url, timeout=20)
        if response.status_code != 200: 
            print(f"GB Skip: HTTP {response.status_code}")
            return None
        
        df = pd.read_csv(io.StringIO(response.text), skipinitialspace=True)
        df.columns = df.columns.str.strip().str.replace('"', '').str.replace("'", "")
        
        # Super-flexible column finding
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        # Look for 'price', 'value', or 'index'
        price_col = next((c for c in df.columns if any(k in c.lower() for k in ['price', 'value', 'index'])), None)
        period_col = next((c for c in df.columns if 'period' in c.lower()), None)
        
        if not date_col or not price_col:
            print(f"GB Skip: Could not find columns. Columns are: {list(df.columns)}")
            return None

        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True)
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
        
        if period_col:
            df['Time'] = df.apply(lambda x: x[date_col] + timedelta(minutes=30 * (int(x[period_col]) - 1)), axis=1)
        else:
            df['Time'] = df[date_col]
            
        df = df.set_index('Time')[price_col].sort_index()
        return df[start_date.tz_localize(None) : end_date.tz_localize(None)]
    except Exception as e:
        print(f"GB Error: {e}")
        return None

all_country_data = []

for code in countries:
    try:
        print(f"Fetching {code}...")
        raw_series = None
        
        if code == 'GB':
            raw_series = fetch_gb_direct_csv(start, end)
        else:
            if ENTSOE_API_KEY:
                raw_series = client.query_day_ahead_prices(code, start=start, end=end)
            
        if raw_series is not None and not raw_series.empty:
            processed = process_to_long_format(raw_series, code)
            all_country_data.append(processed)
        
        time.sleep(1.2) 
    except Exception as e:
        print(f"Skipping {code} due to error: {e}")

if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df = final_df.dropna(subset=['Price'])
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv('market_prices.csv', index=False)
    print("Success: market_prices.csv updated.")
else:
    # This prevents the "Exit Code 1" if NO data is found, by exiting gracefully
    print("Warning: No data collected for any country.")

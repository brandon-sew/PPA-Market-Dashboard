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
    
    # Strictly remove zeros (which indicate missing data in these legacy feeds)
    price_series = price_series[price_series > 0.1]
    
    if price_series.empty: return pd.DataFrame()

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
    """Parses the complex Elexon MID file by looking for specific markers."""
    if not ELEXON_TOKEN: return None
    try:
        url = f"https://downloads.elexonportal.co.uk/file/download/LATEST_MID_FILE?key={ELEXON_TOKEN}"
        r = requests.get(url, timeout=20)
        if r.status_code != 200: return None
        
        # Elexon MID files often contain multiple data types. 
        # We search line-by-line for the 'MID' price data.
        lines = r.text.splitlines()
        extracted_data = []
        
        for line in lines:
            parts = [p.strip().replace('"', '').replace("'", "") for p in line.split(',')]
            # MID rows usually look like: MID, Date, Period, Price...
            if 'MID' in parts or 'Market Index Data' in line:
                try:
                    # Logic: Find a date-looking thing and a number-looking thing
                    # Standard MID format: [Type, Date, Period, Provider, Price, Volume]
                    d_str = next(p for p in parts if '/' in p or '-' in p)
                    nums = [float(p) for p in parts if p.replace('.','',1).isdigit()]
                    if len(nums) >= 2:
                        extracted_data.append({
                            'Date': pd.to_datetime(d_str, dayfirst=True),
                            'Period': int(nums[0]),
                            'Price': float(nums[-1] if nums[-1] > 1 else nums[-2]) # Grab the price, not the volume
                        })
                except: continue

        if not extracted_data: return None
        
        df = pd.DataFrame(extracted_data)
        df = df[df['Price'] > 1.0] # Ignore near-zero values
        
        # Create Time index (30 min periods)
        df['Time'] = df.apply(lambda x: x['Date'] + timedelta(minutes=30 * (x['Period'] - 1)), axis=1)
        df = df.set_index('Time')['Price'].sort_index()
        
        return df[start_date.tz_localize(None) : end_date.tz_localize(None)]
    except Exception as e:
        print(f"GB Detail Error: {e}")
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

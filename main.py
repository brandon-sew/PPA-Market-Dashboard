import os
import time
import pandas as pd
import io
import requests
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Config - SECURITY UPDATE
# This looks for the keys in your system environment variables.
# If they aren't found, it falls back to None to prevent crashes.
ENTSOE_API_KEY = os.environ.get('ENTSOE_TOKEN')
ELEXON_KEY = os.environ.get('ELEXON_KEY') 

client = EntsoePandasClient(api_key=ENTSOE_API_KEY)

# Comprehensive European Bidding Zones
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
    """Calculates Baseload, Peak, and Off-Peak from a time series."""
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
    """Special fetcher for GB using the Elexon Portal Scripting Key."""
    if not ELEXON_KEY:
        print("Error: ELEXON_KEY environment variable not set.")
        return None
        
    try:
        url = f"https://downloads.elexonportal.co.uk/file/download/LATEST_MID_FILE?key={ELEXON_KEY}"
        response = requests.get(url, timeout=20)
        if response.status_code != 200: return None
        
        df = pd.read_csv(io.StringIO(response.text), skipinitialspace=True)
        df.columns = df.columns.str.strip().str.replace('"', '').str.replace("'", "")
        
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        price_col = next((c for c in df.columns if 'price' in c.lower()), None)
        period_col = next((c for c in df.columns if 'period' in c.lower()), None)
        
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True)
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
        
        df['Time'] = df.apply(lambda x: x[date_col] + timedelta(minutes=30 * (int(x[period_col]) - 1)), axis=1)
        df = df.set_index('Time')[price_col].sort_index()
        
        return df[start_date.tz_localize(None) : end_date.tz_localize(None)]
    except Exception as e:
        print(f"GB CSV Fetch Error: {e}")
        return None

import os
import time
import pandas as pd
import io
import requests
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

# 1. Config 
# We now use ELEXON_TOKEN to match your GitHub Secrets naming convention
ENTSOE_API_KEY = os.environ.get('ENTSOE_TOKEN')
ELEXON_TOKEN = os.environ.get('ELEXON_TOKEN') 

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
    """Special fetcher for GB using the Elexon Portal Scripting Token."""
    if not ELEXON_TOKEN:
        print("Error: ELEXON_TOKEN environment variable not set in GitHub Secrets.")
        return None
        
    try:
        # The key parameter in the URL now uses the ELEXON_TOKEN variable
        url = f"https://downloads.elexonportal.co.uk/file/download/LATEST_MID_FILE?key={ELEXON_TOKEN}"
        response = requests.get(url, timeout=20)
        if response.status_code != 200: 
            print(f"GB Fetch Failed: Status {response.status_code}")
            return None
        
        df = pd.read_csv(io.StringIO(response.text), skipinitialspace=True)
        df.columns = df.columns.str.strip().str.replace('"', '').str.replace("'", "")
        
        date_

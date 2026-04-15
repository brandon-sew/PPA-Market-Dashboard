import os
import time
import pandas as pd
from datetime import datetime
from entsoe import EntsoePandasClient

# 1. Config
# Only need the ENTSO-E token now
ENTSOE_API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=ENTSOE_API_KEY)

# Comprehensive list of bidding zones matching your app.py mapping
countries = [
    # Central & Western Europe
    'AT', 'BE', 'CH', 'CZ', 'DE_LU', 'FR', 'GB', 'IE_SEM', 'NL', 'PL',
    
    # Northern Europe
    'DK_1', 'DK_2', 'EE', 'FI', 'LT', 'LV', 
    'NO_1', 'NO_2', 'NO_3', 'NO_4', 'NO_5', 
    'SE_1', 'SE_2', 'SE_3', 'SE_4',
    
    # Southern & Eastern Europe
    'BG', 'ES', 'GR', 'HR', 'HU', 'PT', 'RO', 'RS', 'SI', 'SK',
    
    # Italy (Complete list from screenshots)
    'IT_NORD', 'IT_CNOR', 'IT_CSUD', 'IT_SUD', 'IT_SICI', 'IT_SARD', 
    'IT_CALA', 'IT_SACO_AC', 'IT_SACO_DC', 'IT_BRNN', 'IT_FOGN', 
    'IT_ROSN', 'IT_PRGP', 'IT_MALT'
]

# Set time window (Brussels time is the standard for ENTSO-E)
end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
start = end - pd.Timedelta(days=10)

def process_to_long_format(price_series, country_code):
    if price_series is None or price_series.empty: 
        return pd.DataFrame()
    
    # Ensure index is datetime and drop any missing values
    price_series.index = pd.to_datetime(price_series.index)
    price_series = price_series[price_series.notna()]
    
    # Calculate daily metrics
    # Baseload: Average of all hours in the day
    baseload = price_series.resample('D').mean()
    
    # Peak: Average of 08:00 to 20:00 (Standard European Peak definition)
    peak = price_series.between_time('08:00', '19:59').resample('D').mean()
    
    # Off-Peak: Average of remaining hours
    off_peak_mask = ~price_series.index.isin(price_series.between_time('08:00', '19:59').index)
    off_peak = price_series.loc[off_peak_mask].resample('D').mean()
    
    data = []
    # Build list of records
    for date, val in baseload.items(): 
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Baseload', 'Price': val})
    for date, val in peak.items(): 
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Peak', 'Price': val})
    for date, val in off_peak.items(): 
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Off-Peak', 'Price': val})
    
    res = pd.DataFrame(data)
    res['Country'] = country_code
    return res

all_country_data = []

# 2. Execution Loop
print(f"Starting data fetch for {len(countries)} zones...")

for code in countries:
    try:
        print(f"Fetching {code}...")
        # Every country, including GB and DE_LU, now uses the standard query
        raw_series = client.query_day_ahead_prices(code, start=start, end=end)
            
        if raw_series is not None and not raw_series.empty:
            processed = process_to_long_format(raw_series, code)
            all_country_data.append(processed)
        
        # Respect API rate limits (ENTSO-E is strict)
        time.sleep(1.2) 
    except Exception as e:
        # Print a short version of the error to keep logs clean
        print(f"Skipping {code}: {str(e)[:75]}...")

# 3. Export
if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv('market_prices.csv', index=False)
    print(f"\nSuccess: market_prices.csv updated.")
    print(f"Final file contains {final_df['Country'].nunique()} bidding zones.")
else:
    print("\nNo data collected. Check your API key and connection.")

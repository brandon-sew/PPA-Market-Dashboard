import os
import time
import pandas as pd
from datetime import datetime
from entsoe import EntsoePandasClient

# 1. Config
# Only need the ENTSO-E token now
ENTSOE_API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=ENTSOE_API_KEY)

# Refined country list (LU removed as it is covered by DE)
countries = [
    'AT', 'BE', 'BG', 'CH', 'CZ', 'DE', 'DK_1', 'DK_2', 'EE', 'ES', 'FI', 
    'FR', 'GB', 'GR', 'HR', 'HU', 'IE_SEM', 'IT_NORD', 'IT_CNOR', 'IT_CSUD', 
    'IT_SUD', 'IT_SICI', 'IT_SARD', 'LT', 'LV', 'NL', 'NO_1', 'NO_2', 
    'NO_3', 'NO_4', 'NO_5', 'PL', 'PT', 'RO', 'RS', 'SE_1', 'SE_2', 'SE_3', 
    'SE_4', 'SI', 'SK'
]

# Set time window (Brussels time is the standard for ENTSO-E)
end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
start = end - pd.Timedelta(days=10)

def process_to_long_format(price_series, country_code):
    if price_series is None or price_series.empty: 
        return pd.DataFrame()
    
    price_series.index = pd.to_datetime(price_series.index)
    price_series = price_series[price_series.notna()]
    
    # Calculate daily metrics
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

all_country_data = []

# 2. Execution Loop
for code in countries:
    try:
        print(f"Fetching {code}...")
        # Every country, including GB, now uses the same query
        raw_series = client.query_day_ahead_prices(code, start=start, end=end)
            
        if raw_series is not None and not raw_series.empty:
            processed = process_to_long_format(raw_series, code)
            all_country_data.append(processed)
        
        # Respect API rate limits
        time.sleep(1.1) 
    except Exception as e:
        print(f"Skipping {code}: {str(e)[:100]}")

# 3. Export
if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv('market_prices.csv', index=False)
    print(f"Success: market_prices.csv updated with {len(countries)} countries.")
else:
    print("No data collected. Check your API key.")

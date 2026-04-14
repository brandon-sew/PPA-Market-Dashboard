import os
import time
import pandas as pd
from datetime import datetime
from entsoe import EntsoePandasClient

# 1. Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)

# Expanded list covering almost all major ENTSO-E zones
# Split DE and LU included here
countries = [
    'AT', 'BE', 'CH', 'CZ', 'DE', 'DK_1', 'DK_2', 'EE', 'ES', 'FI', 
    'FR', 'GB', 'GR', 'HR', 'HU', 'IE_SEM', 'IT_GR', 'LT', 'LU', 'LV', 
    'NL', 'NO_1', 'NO_2', 'NO_3', 'NO_4', 'NO_5', 'PL', 'PT', 'RO', 
    'SE_1', 'SE_2', 'SE_3', 'SE_4', 'SI', 'SK'
]

end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
start = end - pd.Timedelta(days=10) # 10-day window for a good balance

def process_to_long_format(price_series, country_code):
    if price_series is None or price_series.empty:
        return pd.DataFrame()
    
    # Standard Baseload
    baseload = price_series.resample('D').mean()
    
    # Peak (08:00 - 20:00)
    peak = price_series.between_time('08:00', '19:59').resample('D').mean()
    
    # Off-Peak
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

# 2. Execution Loop
all_country_data = []
for code in countries:
    try:
        print(f"Fetching {code}...")
        raw_series = client.query_day_ahead_prices(code, start=start, end=end)
        processed = process_to_long_format(raw_series, code)
        all_country_data.append(processed)
        
        # Respectful pause to prevent API rate-limiting
        time.sleep(1.5) 
        
    except Exception as e:
        print(f"Skipping {code}: {e}")

# 3. Final Save
if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2) # Keep it to 2 decimals
    final_df.to_csv('market_prices.csv', index=False)
    print(f"Success! market_prices.csv updated with {len(countries)} zones.")
else:
    print("Warning: No data was found for any country.")

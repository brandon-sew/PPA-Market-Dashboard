import os
import pandas as pd
import numpy as np
from datetime import datetime
from entsoe import EntsoePandasClient

# 1. Configuration & Secrets
API_KEY = os.environ.get('ENTSOE_TOKEN')

# If no API key is found, the script will fail early to let you know
if not API_KEY:
    raise ValueError("ENTSOE_TOKEN not found. Please add it to GitHub Secrets.")

client = EntsoePandasClient(api_key=API_KEY)

# 2. Define Countries & Timeframe
# You can add or remove country codes here as needed
countries = ['DE_LU', 'FR', 'ES', 'PL', 'NL', 'IT_SACO_AC']

end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
start = end - pd.Timedelta(days=14)  # Fetch last 14 days for a better trend graph

def process_to_long_format(price_series, country_code):
    """Processes raw hourly data into Baseload, Peak, and Off-Peak averages."""
    # Daily Baseload
    baseload = price_series.resample('D').mean()
    
    # Peak (08:00 - 20:00)
    peak = price_series.between_time('08:00', '19:59').resample('D').mean()
    
    # Off-Peak (Everything else)
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

# 3. Main Execution Loop
all_country_data = []

for code in countries:
    try:
        print(f"Fetching data for {code}...")
        raw_series = client.query_day_ahead_prices(code, start=start, end=end)
        processed_df = process_to_long_format(raw_series, code)
        all_country_data.append(processed_df)
    except Exception as e:
        print(f"Error fetching {code}: {e}")

# 4. Save the Final CSV
if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2)
    
    # Reorder columns for a clean CSV
    final_df = final_df[['Date', 'Country', 'Metric', 'Price']]
    
    # Save to the same folder so app.py can find it
    final_df.to_csv('market_prices.csv', index=False)
    print("market_prices.csv successfully updated.")
else:
    print("No data was collected.")

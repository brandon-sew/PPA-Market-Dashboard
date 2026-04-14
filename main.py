import os
import pandas as pd
from datetime import datetime
from entsoe import EntsoePandasClient

# 1. Config
API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=API_KEY)
# Using fewer countries for the first test run
countries = ['DE_LU', 'FR', 'ES'] 

end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
start = end - pd.Timedelta(days=7)

def process_to_long_format(price_series, country_code):
    if price_series is None or price_series.empty:
        return pd.DataFrame()
    
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

# 2. Execution
all_country_data = []
for code in countries:
    try:
        print(f"Fetching {code}...")
        raw_series = client.query_day_ahead_prices(code, start=start, end=end)
        processed = process_to_long_format(raw_series, code)
        all_country_data.append(processed)
    except Exception as e:
        print(f"Skipping {code}: {e}")

# 3. Final Save
if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv('market_prices.csv', index=False)
    print("Success: market_prices.csv updated.")
else:
    print("Error: No data fetched.")

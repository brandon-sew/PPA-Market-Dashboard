import os
import pandas as pd
import time
from datetime import datetime
from entsoe import EntsoePandasClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. Config
ENTSOE_API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=ENTSOE_API_KEY)

countries = [
    'AT', 'BE', 'CH', 'CZ', 'DE_LU', 'FR', 'GB', 'IE_SEM', 'NL', 'PL',
    'DK_1', 'DK_2', 'EE', 'FI', 'LT', 'LV', 'NO_1', 'NO_2', 'NO_3', 
    'NO_4', 'NO_5', 'SE_1', 'SE_2', 'SE_3', 'SE_4', 'BG', 'ES', 'GR', 
    'HR', 'HU', 'ME', 'MK', 'PT', 'RO', 'RS', 'SI', 'SK', 'IT_NORD', 
    'IT_CNOR', 'IT_CSUD', 'IT_SUD', 'IT_SICI', 'IT_SARD', 'IT_CALA'
]

# --- CHANGE FOR HEAVY LIFTING ---
end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
# Set to 5 years (5 * 365 days)
start = end - pd.Timedelta(days=5*365) 
# --------------------------------

def process_metrics(price_series, gen_df, country_code):
    if price_series is None or price_series.empty: 
        return pd.DataFrame()
    
    price_series.index = pd.to_datetime(price_series.index)
    price_series = price_series[price_series.notna()]
    
    baseload = price_series.resample('D').mean()
    data = []
    
    for date, val in baseload.items(): 
        if pd.notna(val): 
            data.append({'Date': date.date(), 'Metric': 'Baseload', 'Price': val})

    if gen_df is not None and not gen_df.empty:
        if isinstance(gen_df.columns, pd.MultiIndex):
            gen_df.columns = gen_df.columns.get_level_values(0)
        
        combined = pd.merge(price_series.to_frame('Price'), gen_df, left_index=True, right_index=True, how='inner')
        
        # Generation capture price logic
        gen_types = {
            'Solar': 'Solar Capture', 
            'Wind Onshore': 'Wind Onshore Capture', 
            'Wind Offshore': 'Wind Offshore Capture'
        }
        
        for g_col, g_metric in gen_types.items():
            if g_col in combined.columns:
                def calc_cap(group):
                    total_gen = group[g_col].sum()
                    return (group['Price'] * group[g_col]).sum() / total_gen if total_gen > 0 else None
                
                cap_series = combined.resample('D').apply(calc_cap)
                for date, val in cap_series.items():
                    if pd.notna(val): 
                        data.append({'Date': date.date(), 'Metric': g_metric, 'Price': val})

    res = pd.DataFrame(data)
    res['Country'] = country_code
    return res

def fetch_single_country(code):
    try:
        # entsoe-py automatically splits 5 years into 1-year chunks for you
        raw_series = client.query_day_ahead_prices(code, start=start, end=end)
        
        try:
            gen_df = client.query_generation(code, start=start, end=end)
        except:
            gen_df = None
            
        if raw_series is not None and not raw_series.empty:
            return process_metrics(raw_series, gen_df, code)
    except Exception as e:
        print(f"Error fetching {code}: {str(e)[:50]}")
    return pd.DataFrame()

# 2. Parallel Execution
all_country_data = []
print(f"🚀 INITIALIZING 5-YEAR BULK FETCH (from {start.date()} to {end.date()})")
print("This may take 1-2 hours depending on API congestion...")

# Using 3 workers for the 5-year fetch to avoid hitting rate limits on massive transfers
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {executor.submit(fetch_single_country, code): code for code in countries}
    for i, future in enumerate(as_completed(futures)):
        result = future.result()
        if not result.empty:
            all_country_data.append(result)
        print(f"Progress: {i+1}/{len(countries)} zones completed.")

# 3. Export
if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv('market_prices.csv', index=False)
    print(f"\n✅ SUCCESS: 5 years of data saved to market_prices.csv.")
else:
    print("\n❌ FAILED: No data collected.")

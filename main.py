import os
import pandas as pd
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

# --- SET FOR 5-YEAR HEAVY LIFT ---
end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
start = end - pd.Timedelta(days=5*365) 
csv_filename = 'market_prices.csv'
# ---------------------------------

def process_metrics(price_series, gen_df, country_code):
    if price_series is None or price_series.empty: 
        return pd.DataFrame()
    
    price_series.index = pd.to_datetime(price_series.index)
    data = []
    
    # 1. Baseload Price
    baseload = price_series.resample('D').mean()
    for date, val in baseload.items():
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Baseload', 'Price': val})

    if gen_df is not None and not gen_df.empty:
        if isinstance(gen_df.columns, pd.MultiIndex):
            gen_df.columns = gen_df.columns.get_level_values(0)
        
        # Aggregate duplicates (some zones have multiple categories for same fuel)
        gen_df = gen_df.T.groupby(level=0).sum().T
        combined = pd.merge(price_series.to_frame('Price'), gen_df, left_index=True, right_index=True, how='inner')
        
        mapping = {
            'Solar': 'Solar',
            'Wind Onshore': 'Wind Onshore',
            'Wind Offshore': 'Wind Offshore'
        }
        
        for fuel, label in mapping.items():
            if fuel in combined.columns:
                # 2. Add Generation Volume (MWh)
                daily_gen = combined[fuel].resample('D').sum()
                for date, val in daily_gen.items():
                    if pd.notna(val): data.append({'Date': date.date(), 'Metric': f'{label} Generation', 'Price': val})
                
                # 3. Add Capture Price (€/MWh)
                def calc_cap(group):
                    total_mwh = group[fuel].sum()
                    return (group['Price'] * group[fuel]).sum() / total_mwh if total_mwh > 0 else None
                
                cap_series = combined.resample('D').apply(calc_cap)
                for date, val in cap_series.items():
                    if pd.notna(val): data.append({'Date': date.date(), 'Metric': f'{label} Capture', 'Price': val})

    res = pd.DataFrame(data)
    res['Country'] = country_code
    return res

def fetch_single_country(code):
    try:
        print(f"Fetching {code}...")
        # Prices
        prices = client.query_day_ahead_prices(code, start=start, end=end)
        # Actual Generation (used for historical Capture Prices)
        try:
            gen = client.query_generation(code, start=start, end=end)
        except:
            gen = None
        return process_metrics(prices, gen, code)
    except Exception as e:
        print(f"Error {code}: {str(e)[:50]}")
        return pd.DataFrame()

# Parallel Execution
all_country_data = []
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {executor.submit(fetch_single_country, code): code for code in countries}
    for i, future in enumerate(as_completed(futures)):
        result = future.result()
        if not result.empty:
            all_country_data.append(result)
        print(f"Progress: {i+1}/{len(countries)} zones finished.")

# Export (Overwrite the old price-only CSV)
if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv(csv_filename, index=False)
    print(f"✅ Success: 5-year complete data saved to {csv_filename}")

import os
import time
import pandas as pd
from datetime import datetime
from entsoe import EntsoePandasClient

# 1. Config
ENTSOE_API_KEY = os.environ.get('ENTSOE_TOKEN')
client = EntsoePandasClient(api_key=ENTSOE_API_KEY)

countries = [
    # Central & Western Europe
    'AT', 'BE', 'CH', 'CZ', 'DE_LU', 'FR', 'GB', 'IE_SEM', 'NL', 'PL',
    
    # Northern Europe
    'DK_1', 'DK_2', 'EE', 'FI', 'LT', 'LV', 
    'NO_1', 'NO_2', 'NO_3', 'NO_4', 'NO_5', 
    'SE_1', 'SE_2', 'SE_3', 'SE_4',
    
    # Southern & Eastern Europe
    'BG', 'ES', 'GR', 'HR', 'HU', 'ME', 'MK', 'PT', 'RO', 'RS', 'SI', 'SK',
    
    # Italy
    'IT_NORD', 'IT_CNOR', 'IT_CSUD', 'IT_SUD', 'IT_SICI', 'IT_SARD', 
    'IT_CALA'
]

end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')
start = end - pd.Timedelta(days=10)

def process_metrics(price_series, gen_df, country_code):
    if price_series is None or price_series.empty: 
        return pd.DataFrame()
    
    price_series.index = pd.to_datetime(price_series.index)
    price_series = price_series[price_series.notna()]
    
    # Daily Baseload
    baseload = price_series.resample('D').mean()
    
    # Prep for Capture Price calculations
    data = []
    
    # Add Baseload to data
    for date, val in baseload.items(): 
        if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Baseload', 'Price': val})

    # Capture Prices
    if gen_df is not None and not gen_df.empty:
        if isinstance(gen_df.columns, pd.MultiIndex):
            gen_df.columns = gen_df.columns.get_level_values(0)
        
        # Merge price and generation on time
        combined = pd.merge(price_series.to_frame('Price'), gen_df, left_index=True, right_index=True, how='inner')
        
        # Solar Capture Price
        if 'Solar' in combined.columns:
            def calc_solar(group):
                total_gen = group['Solar'].sum()
                return (group['Price'] * group['Solar']).sum() / total_gen if total_gen > 0 else None
            
            solar_cap = combined.resample('D').apply(calc_solar)
            for date, val in solar_cap.items():
                if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Solar Capture', 'Price': val})

        # Wind Onshore Capture Price
        if 'Wind Onshore' in combined.columns:
            def calc_onshore(group):
                total_gen = group['Wind Onshore'].sum()
                return (group['Price'] * group['Wind Onshore']).sum() / total_gen if total_gen > 0 else None
            onshore_cap = combined.resample('D').apply(calc_onshore)
            for date, val in onshore_cap.items():
                if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Wind Onshore Capture', 'Price': val})

        # Wind Offshore Capture Price
        if 'Wind Offshore' in combined.columns:
            def calc_offshore(group):
                total_gen = group['Wind Offshore'].sum()
                return (group['Price'] * group['Wind Offshore']).sum() / total_gen if total_gen > 0 else None
            offshore_cap = combined.resample('D').apply(calc_offshore)
            for date, val in offshore_cap.items():
                if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Wind Offshore Capture', 'Price': val})

    res = pd.DataFrame(data)
    res['Country'] = country_code
    return res

all_country_data = []

print(f"Starting data fetch for {len(countries)} zones...")

for code in countries:
    try:
        print(f"Fetching {code}...")
        raw_series = client.query_day_ahead_prices(code, start=start, end=end)
        
        try:
            gen_df = client.query_generation(code, start=start, end=end)
            # Fix duplicate columns for Austria/NL
            if gen_df is not None:
                if isinstance(gen_df.columns, pd.MultiIndex):
                    gen_df.columns = gen_df.columns.get_level_values(0)
                gen_df = gen_df.T.groupby(level=0).sum().T
        except:
            gen_df = None
            
        if raw_series is not None and not raw_series.empty:
            processed = process_metrics(raw_series, gen_df, code)
            all_country_data.append(processed)
        
        time.sleep(1.5) 
    except Exception as e:
        print(f"Skipping {code}: {str(e)[:75]}...")

# 3. Export
if all_country_data:
    final_df = pd.concat(all_country_data, ignore_index=True)
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv('market_prices.csv', index=False)
    print(f"\nSuccess: market_prices.csv updated.")
else:
    print("\nNo data collected.")

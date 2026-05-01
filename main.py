import os
import pandas as pd
from datetime import datetime
from entsoe import EntsoePandasClient
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# --- HYBRID LOGIC START ---
csv_filename = 'market_prices.csv'
end = pd.Timestamp(datetime.now(), tz='Europe/Brussels')

if os.path.exists(csv_filename):
    # Load existing long-term data
    existing_df = pd.read_csv(csv_filename)
    existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.date
    # API "Deep Dive" starts from the last available date in CSV
    start = pd.Timestamp(existing_df['Date'].max(), tz='Europe/Brussels')
else:
    existing_df = pd.DataFrame()
    # Initial "Long-term" fetch (e.g., 365 days)
    start = end - pd.Timedelta(days=365)
# --- HYBRID LOGIC END ---

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
        
        if 'Solar' in combined.columns:
            def calc_solar(group):
                total_gen = group['Solar'].sum()
                return (group['Price'] * group['Solar']).sum() / total_gen if total_gen > 0 else None
            solar_cap = combined.resample('D').apply(calc_solar)
            for date, val in solar_cap.items():
                if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Solar Capture', 'Price': val})

        if 'Wind Onshore' in combined.columns:
            def calc_onshore(group):
                total_gen = group['Wind Onshore'].sum()
                return (group['Price'] * group['Wind Onshore']).sum() / total_gen if total_gen > 0 else None
            onshore_cap = combined.resample('D').apply(calc_onshore)
            for date, val in onshore_cap.items():
                if pd.notna(val): data.append({'Date': date.date(), 'Metric': 'Wind Onshore Capture', 'Price': val})

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

def fetch_single_country(code):
    """Worker function to handle data fetching for one country."""
    try:
        print(f"Fetching {code}...")
        raw_series = client.query_day_ahead_prices(code, start=start, end=end)
        
        try:
            gen_df = client.query_generation(code, start=start, end=end)
            if gen_df is not None:
                if isinstance(gen_df.columns, pd.MultiIndex):
                    gen_df.columns = gen_df.columns.get_level_values(0)
                gen_df = gen_df.T.groupby(level=0).sum().T
        except:
            gen_df = None
            
        if raw_series is not None and not raw_series.empty:
            return process_metrics(raw_series, gen_df, code)
    except Exception as e:
        print(f"Skipping {code}: {str(e)[:75]}...")
    return pd.DataFrame()

# 2. Parallel Execution
all_country_data = []
print(f"Starting parallel data fetch for {len(countries)} zones...")

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(fetch_single_country, code): code for code in countries}
    for future in as_completed(futures):
        result = future.result()
        if not result.empty:
            all_country_data.append(result)

# 3. Export with Merge Logic
if all_country_data:
    new_df = pd.concat(all_country_data, ignore_index=True)
    
    # Combine new API data with old CSV data
    final_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    # Ensure Date format consistency and drop overlaps
    final_df['Date'] = pd.to_datetime(final_df['Date']).dt.date
    final_df = final_df.drop_duplicates(subset=['Date', 'Metric', 'Country'], keep='last')
    final_df = final_df.sort_values(by=['Country', 'Date'])
    
    final_df['Price'] = final_df['Price'].round(2)
    final_df.to_csv(csv_filename, index=False)
    print(f"\nSuccess: {csv_filename} updated with recent API data.")
else:
    print("\nNo new data collected.")

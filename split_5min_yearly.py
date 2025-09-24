import pandas as pd
import os

# Timeframe to split
timeframe = '5min'
folder = f'data/btc/{timeframe}'
full_file = f'{folder}/BTCUSD_5m_candles_full.csv'

# Read the full CSV
df = pd.read_csv(full_file)

# Extract year
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['year'] = df['timestamp'].dt.year

# Group by year and save
for year in df['year'].unique():
    df_year = df[df['year'] == year].copy()
    df_year.drop('year', axis=1, inplace=True)
    df_year['timestamp'] = df_year['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    output_file = f'{folder}/BTCUSD_5m_candles_{year}.csv'
    df_year.to_csv(output_file, index=False)
    print(f'Saved {output_file}')

print("Yearly split complete for 5min.")
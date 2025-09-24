import pandas as pd
import os
import glob

# Path to the BTC data folder
path = 'data/btc/'

# Get all minute candle files
files = glob.glob(os.path.join(path, 'BTCUSD_1m_candles_*.csv'))

# Combine all data
df_list = []
for file in files:
    df = pd.read_csv(file)
    df_list.append(df)

df_all = pd.concat(df_list, ignore_index=True)

# Convert timestamp to datetime and sort
df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
df_all = df_all.sort_values('unix_timestamp').reset_index(drop=True)

# Set timestamp as index
df_all.set_index('timestamp', inplace=True)

# Define timeframes and filenames
timeframes = {
    '5min': '5min',
    '30min': '30min',
    'hourly': 'h',
    'daily': 'D',
    'weekly': 'W'
}

filenames = {
    '5min': 'BTCUSD_5m_candles_full.csv',
    '30min': 'BTCUSD_30m_candles_full.csv',
    'hourly': 'BTCUSD_1h_candles_full.csv',
    'daily': 'BTCUSD_1d_candles_full.csv',
    'weekly': 'BTCUSD_1w_candles_full.csv'
}

# Aggregate for each timeframe
for tf, freq in timeframes.items():
    folder = os.path.join(path, tf)
    os.makedirs(folder, exist_ok=True)
    
    df_resampled = df_all.resample(freq).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # Add unix_timestamp
    df_resampled['unix_timestamp'] = (df_resampled.index.astype('int64') // 10**9)
    
    # Format timestamp
    df_resampled['timestamp'] = df_resampled.index.strftime('%Y-%m-%d %H:%M:%S')
    
    # Reorder columns
    df_resampled = df_resampled[['timestamp', 'open', 'close', 'volume', 'unix_timestamp', 'high', 'low']]
    
    # Save to CSV
    df_resampled.to_csv(os.path.join(folder, filenames[tf]), index=False)

print("Aggregation complete!")
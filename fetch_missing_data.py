import requests
import time
import json
import csv
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Configuration - Using Binance API for better historical data coverage
BASE_URL = 'https://api.binance.com/api/v3/klines'
SYMBOL = 'BTCUSDT'
INTERVAL = '1m'
MAX_CANDLES_PER_REQUEST = 1000  # Binance allows up to 1000
MAX_WORKERS = 10  # Conservative for Binance API
RATE_LIMIT_DELAY = 0.1
MAX_RETRIES = 5
RETRY_DELAY = 2

def get_candles_binance(start_unix, end_unix, retry_count=0):
    """
    Fetch candle data from Binance API with robust retry logic
    """
    start_ms = start_unix * 1000
    end_ms = end_unix * 1000
    
    params = {
        'symbol': SYMBOL,
        'interval': INTERVAL,
        'startTime': start_ms,
        'endTime': end_ms,
        'limit': MAX_CANDLES_PER_REQUEST
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if not data:
                return None
            
            # Convert Binance format to Coinbase format
            # Binance: [timestamp, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore]
            # Coinbase: [timestamp, low, high, open, close, volume]
            converted_data = []
            for kline in data:
                timestamp = int(kline[0]) // 1000  # Convert ms to seconds
                open_price = float(kline[1])
                high = float(kline[2])
                low = float(kline[3])
                close = float(kline[4])
                volume = float(kline[5])
                
                converted_data.append([timestamp, low, high, open_price, close, volume])
            
            return converted_data
            
        elif response.status_code == 429:  # Rate limit exceeded
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY * (2 ** retry_count)
                print(f'   ⏳ Binance rate limit hit. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
                time.sleep(retry_delay)
                return get_candles_binance(start_unix, end_unix, retry_count + 1)
            else:
                print(f'   ❌ Binance rate limit exceeded. Max retries reached.')
                return None
        else:
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY * (2 ** retry_count)
                print(f'   ⚠️  Binance request failed: {response.status_code}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
                time.sleep(retry_delay)
                return get_candles_binance(start_unix, end_unix, retry_count + 1)
            else:
                print(f'   ❌ Binance request failed: {response.status_code}. Max retries reached.')
                return None
                
    except Exception as e:
        if retry_count < MAX_RETRIES:
            retry_delay = RETRY_DELAY * (2 ** retry_count)
            print(f'   🔄 Binance request exception: {e}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
            time.sleep(retry_delay)
            return get_candles_binance(start_unix, end_unix, retry_count + 1)
        else:
            print(f'   ❌ Binance request exception: {e}. Max retries reached.')
            return None

def fill_missing_candles(candles, start_timestamp, end_timestamp):
    """
    Fill missing candles with zero volume and copied prices
    """
    filled = []
    expected_time = start_timestamp
    last_candle = None

    # Sort candles by timestamp ascending
    candles = sorted(candles, key=lambda c: c[0])

    idx = 0
    while expected_time <= end_timestamp:
        if idx < len(candles) and candles[idx][0] == expected_time:
            # Actual candle present
            last_candle = candles[idx]
            filled.append(last_candle)
            idx += 1
        else:
            # Missing candle, copy last candle but zero volume
            if last_candle:
                filled.append([
                    expected_time,          # time
                    last_candle[1],        # low
                    last_candle[2],        # high
                    last_candle[3],        # open
                    last_candle[4],        # close
                    0.0                    # volume zero for missing data
                ])
            else:
                # No previous candle, insert a zero-volume dummy candle
                filled.append([expected_time, 0, 0, 0, 0, 0])
        expected_time += 60  # 60 seconds for 1-minute intervals

    return filled

def fetch_missing_range(range_info, range_id):
    """
    Fetch data for a missing range
    """
    start_ts = range_info['start_timestamp']
    end_ts = range_info['end_timestamp']
    duration_minutes = range_info['duration_minutes']
    
    print(f"   📡 Fetching range {range_id}: {range_info['start_datetime']} to {range_info['end_datetime']} ({duration_minutes} minutes)")
    
    try:
        # Add small delay to avoid rate limits
        time.sleep(RATE_LIMIT_DELAY * range_id)
        
        # For large ranges, break them into smaller chunks
        MAX_CHUNK_SIZE = MAX_CANDLES_PER_REQUEST * 60  # 1000 minutes for Binance
        chunks = []
        
        current_start = start_ts
        while current_start <= end_ts:
            current_end = min(current_start + MAX_CHUNK_SIZE - 60, end_ts)
            chunks.append((current_start, current_end))
            current_start = current_end + 60
        
        all_data = []
        
        for chunk_start, chunk_end in chunks:
            raw_data = get_candles_binance(chunk_start, chunk_end)
            if raw_data is None:
                print(f"   ❌ Failed to fetch chunk from Binance: {datetime.fromtimestamp(chunk_start, timezone.utc)} to {datetime.fromtimestamp(chunk_end, timezone.utc)}")
                continue
            
            # Fill missing candles in this chunk
            filled_data = fill_missing_candles(raw_data, chunk_start, chunk_end)
            all_data.extend(filled_data)
        
        if not all_data:
            print(f"   ❌ No data retrieved for range {range_id}")
            return None, range_info
        
        # Convert to CSV format
        csv_rows = []
        for candle in all_data:
            timestamp_unix = candle[0]
            timestamp_iso = datetime.fromtimestamp(timestamp_unix, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            low = candle[1]
            high = candle[2]
            open_p = candle[3]
            close = candle[4]
            volume = candle[5]
            csv_rows.append({
                'timestamp': timestamp_iso,
                'open': open_p,
                'close': close,
                'volume': volume,
                'unix_timestamp': timestamp_unix,
                'high': high,
                'low': low
            })
        
        print(f"   ✅ Successfully retrieved {len(csv_rows)} candles for range {range_id}")
        return csv_rows, range_info
        
    except Exception as e:
        print(f"   ❌ Exception in range {range_id}: {e}")
        return None, range_info

def main():
    if len(sys.argv) != 2:
        print("Usage: python fetch_missing_data.py <ranges_json_file>")
        print("Example: python fetch_missing_data.py BTCUSD_1m_candles_2018_missing_ranges.json")
        sys.exit(1)
    
    ranges_file = sys.argv[1]
    
    print("🚀 Missing Data Fetcher (Using Binance API)")
    print("="*60)
    
    # Load missing ranges
    try:
        with open(ranges_file, 'r') as f:
            ranges_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: File {ranges_file} not found!")
        print("Run find_missing_data.py first to generate the ranges file.")
        sys.exit(1)
    
    original_filename = ranges_data['filename']
    ranges = ranges_data['ranges']
    total_missing = ranges_data['total_missing_timestamps']
    
    print(f"📋 Original file: {original_filename}")
    print(f"📊 Missing timestamps: {total_missing:,}")
    print(f"📦 Missing ranges: {len(ranges)}")
    print(f"👥 Using {MAX_WORKERS} workers")
    print("="*60)
    
    # Fetch missing data
    all_missing_data = []
    successful_ranges = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_range = {
            executor.submit(fetch_missing_range, range_info, i+1): (range_info, i+1)
            for i, range_info in enumerate(ranges)
        }
        
        # Process completed tasks
        for future in as_completed(future_to_range):
            range_info, range_id = future_to_range[future]
            try:
                csv_rows, _ = future.result()
                if csv_rows is not None:
                    all_missing_data.extend(csv_rows)
                    successful_ranges += 1
                
                # Progress update
                print(f"   📈 Progress: {successful_ranges}/{len(ranges)} ranges completed")
                
            except Exception as e:
                print(f"   ❌ Exception in range {range_id}: {e}")
    
    if not all_missing_data:
        print("\n❌ No missing data could be retrieved.")
        sys.exit(1)
    
    # Sort by timestamp
    all_missing_data.sort(key=lambda x: x['unix_timestamp'])
    
    # Save to CSV file
    output_filename = original_filename.replace('.csv', '_missing_data.csv')
    
    print(f"\n💾 Saving {len(all_missing_data):,} missing candles to {output_filename}...")
    
    with open(output_filename, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'open', 'close', 'volume', 'unix_timestamp', 'high', 'low']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in all_missing_data:
            writer.writerow(row)
    
    print("\n" + "="*60)
    print("📊 MISSING DATA FETCH SUMMARY")
    print("="*60)
    print(f"✅ Successfully fetched: {successful_ranges}/{len(ranges)} ranges")
    print(f"📈 Retrieved candles: {len(all_missing_data):,}")
    print(f"💾 Saved to: {output_filename}")
    
    if successful_ranges == len(ranges):
        print("🎉 All missing data successfully retrieved!")
    else:
        failed_ranges = len(ranges) - successful_ranges
        print(f"⚠️  {failed_ranges} ranges could not be retrieved (API issues or persistent rate limiting)")
    
    print(f"\n📋 Next steps:")
    print(f"   1. Merge {output_filename} with {original_filename}")
    print(f"   2. Re-run validation to confirm all data is now complete")

if __name__ == "__main__":
    main()
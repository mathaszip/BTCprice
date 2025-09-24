import requests
import time
import subprocess
import os
import json
from datetime import datetime, timedelta, timezone
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# Configuration
BASE_URL = 'https://api.exchange.coinbase.com/products/ETH-USD/candles'
GRANULARITY = 60  # 1 minute in seconds
MAX_CANDLES_PER_REQUEST = 300  # Coinbase limit for 1m granularity
MAX_WORKERS = 50  # Number of parallel workers
RATE_LIMIT_DELAY = 0.025  # Delay between requests to avoid rate limits
MAX_RETRIES = 5  # Maximum number of retries for failed requests
RETRY_DELAY = 2  # Initial delay between retries (seconds)

# Pipeline configuration
STARTING_YEAR = 2016  # ETH started trading on Coinbase in 2016
CURRENT_YEAR = datetime.now().year
ETH_START_DATE = datetime(2016, 5, 23, tzinfo=timezone.utc)  # First ETH trading date

def get_candles(start_unix, end_unix, retry_count=0):
    params = {
        'start': datetime.fromtimestamp(start_unix, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'end': datetime.fromtimestamp(end_unix, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'granularity': GRANULARITY
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return data if data else None
        elif response.status_code == 429:  # Rate limit exceeded
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY * (2 ** retry_count)  # Exponential backoff
                print(f'Rate limit hit. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
                time.sleep(retry_delay)
                return get_candles(start_unix, end_unix, retry_count + 1)
            else:
                print(f'Rate limit exceeded. Max retries ({MAX_RETRIES}) reached for interval {datetime.fromtimestamp(start_unix, timezone.utc)} to {datetime.fromtimestamp(end_unix, timezone.utc)}')
                return None
        else:
            # Other HTTP errors
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY * (2 ** retry_count)
                print(f'Request failed: {response.status_code}, {response.text}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
                time.sleep(retry_delay)
                return get_candles(start_unix, end_unix, retry_count + 1)
            else:
                print(f'Request failed: {response.status_code}, {response.text}. Max retries reached.')
                return None
                
    except Exception as e:
        if retry_count < MAX_RETRIES:
            retry_delay = RETRY_DELAY * (2 ** retry_count)
            print(f'Request exception: {e}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
            time.sleep(retry_delay)
            return get_candles(start_unix, end_unix, retry_count + 1)
        else:
            print(f'Request exception: {e}. Max retries reached.')
            return None

def fill_missing_candles(candles, start_timestamp, end_timestamp):
    filled = []
    expected_time = start_timestamp
    last_candle = None

    # Sort candles by timestamp ascending
    candles = sorted(candles, key=lambda c: c[0])

    idx = 0
    while expected_time < end_timestamp:
        if idx < len(candles) and candles[idx][0] == expected_time:
            # Actual candle present
            last_candle = candles[idx]
            filled.append(last_candle)
            idx += 1
        else:
            # Missing candle, copy last candle but zero volume and set timestamp to expected_time
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
                # No previous candle, insert a zero-volume dummy candle with all prices zero
                filled.append([expected_time, 0, 0, 0, 0, 0])
        expected_time += GRANULARITY

    return filled

def fetch_interval_data(start_unix, end_unix, interval_id):
    """Fetch and process data for a single time interval"""
    try:
        # Stagger requests to avoid overwhelming the API
        stagger_delay = RATE_LIMIT_DELAY * (interval_id % MAX_WORKERS)
        time.sleep(stagger_delay)
        
        raw_data = get_candles(start_unix, end_unix)
        if raw_data is None:
            print(f'⚠️  Failed to get data for interval {interval_id}: {datetime.fromtimestamp(start_unix, timezone.utc)} to {datetime.fromtimestamp(end_unix, timezone.utc)}')
            return None, start_unix, end_unix

        # Fill missing candles with zero-vol and copy last candle price
        data = fill_missing_candles(raw_data, start_unix, end_unix)
        
        # Convert to CSV format
        csv_rows = []
        for candle in data:
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

        print(f'✓ Fetched interval {interval_id}: {datetime.fromtimestamp(start_unix, timezone.utc)} to {datetime.fromtimestamp(end_unix, timezone.utc)} ({len(csv_rows)} candles)')
        return csv_rows, start_unix, end_unix
        
    except Exception as e:
        print(f'✗ Error fetching interval {interval_id}: {e}')
        return None, start_unix, end_unix
    """
    Fetch candle data from Binance API with robust retry logic
    """
    start_ms = start_unix * 1000
    end_ms = end_unix * 1000
    
    params = {
        'symbol': 'ETHUSDT',  # Changed to ETH
        'interval': '1m',
        'startTime': start_ms,
        'endTime': end_ms,
        'limit': 1000  # Binance allows up to 1000
    }
    
    try:
        response = requests.get('https://api.binance.com/api/v3/klines', params=params)
        
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

def find_missing_timestamps(filename):
    """
    Find all missing timestamps in data
    Returns a list of missing timestamp ranges
    """
    print(f"   📊 Analyzing missing timestamps in {filename}...")
    
    timestamps = []
    
    # Read all timestamps from the CSV
    try:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                unix_timestamp = int(row['unix_timestamp'])
                timestamps.append(unix_timestamp)
        
        print(f"   ✓ Read {len(timestamps):,} timestamps")
        
    except FileNotFoundError:
        print(f"   ❌ Error: File {filename} not found!")
        return []
    except Exception as e:
        print(f"   ❌ Error reading file: {e}")
        return []
    
    if not timestamps:
        print("   ❌ No data found in file!")
        return []
    
    # Sort timestamps
    timestamps.sort()
    
    # Find missing timestamp ranges
    missing_ranges = []
    current_missing_start = None
    current_missing_end = None
    
    expected_timestamp = timestamps[0]
    timestamp_set = set(timestamps)
    
    while expected_timestamp <= timestamps[-1]:
        if expected_timestamp not in timestamp_set:
            if current_missing_start is None:
                current_missing_start = expected_timestamp
            current_missing_end = expected_timestamp
        else:
            if current_missing_start is not None:
                missing_ranges.append((current_missing_start, current_missing_end))
                current_missing_start = None
        expected_timestamp += 60
    
    # Don't forget the last range
    if current_missing_start is not None:
        missing_ranges.append((current_missing_start, current_missing_end))
    
    return missing_ranges

def group_consecutive_timestamps(missing_timestamps):
    """
    Group consecutive missing timestamps into ranges for efficient fetching
    """
    if not missing_timestamps:
        return []
    
    ranges = []
    current_start = missing_timestamps[0]
    current_end = missing_timestamps[0]
    
    for i in range(1, len(missing_timestamps)):
        if missing_timestamps[i] == current_end + 60:
            # Consecutive timestamp, extend current range
            current_end = missing_timestamps[i]
        else:
            # Gap found, save current range and start new one
            ranges.append((current_start, current_end))
            current_start = missing_timestamps[i]
            current_end = missing_timestamps[i]
    
    # Don't forget the last range
    ranges.append((current_start, current_end))
    
    return ranges

def fetch_missing_range(range_info, range_id):
    """
    Fetch data for a missing range using Binance
    """
    start_ts = range_info['start_timestamp']
    end_ts = range_info['end_timestamp']
    duration_minutes = range_info['duration_minutes']
    
    print(f"   📡 Fetching range {range_id}: {range_info['start_datetime']} to {range_info['end_datetime']} ({duration_minutes} minutes)")
    
    try:
        # For large ranges, break them into smaller chunks
        MAX_CHUNK_SIZE = 1000 * 60  # 1000 minutes for Binance
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

def merge_csv_files(original_file, missing_data_file, output_file):
    """
    Merge original CSV with missing data CSV and sort by timestamp
    """
    print(f"   🔄 Merging {original_file} with missing data...")
    
    all_data = []
    
    # Read original file
    try:
        with open(original_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                all_data.append(row)
        print(f"   ✅ Read {len(all_data):,} rows from original file")
    except FileNotFoundError:
        print(f"   ❌ Error: {original_file} not found!")
        return False
    
    # Read missing data file (if it exists)
    missing_count = 0
    try:
        with open(missing_data_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                all_data.append(row)
                missing_count += 1
        print(f"   ✅ Read {missing_count:,} rows from missing data file")
    except FileNotFoundError:
        print(f"   ⚠️  Missing data file {missing_data_file} not found - using original data only")
    
    # Sort by unix timestamp
    all_data.sort(key=lambda x: int(x['unix_timestamp']))
    
    # Remove duplicates (keep first occurrence)
    seen_timestamps = set()
    unique_data = []
    duplicates_removed = 0
    
    for row in all_data:
        timestamp = int(row['unix_timestamp'])
        if timestamp not in seen_timestamps:
            seen_timestamps.add(timestamp)
            unique_data.append(row)
        else:
            duplicates_removed += 1
    
    if duplicates_removed > 0:
        print(f"   🧹 Removed {duplicates_removed} duplicate timestamps")
    
    # Write merged file
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'open', 'close', 'volume', 'unix_timestamp', 'high', 'low']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in unique_data:
            writer.writerow(row)
    
    print(f"   💾 Saved {len(unique_data):,} rows to {output_file}")
    return True

def validate_data(filename, expected_start=None, expected_end=None):
    """
    Validate data completeness
    """
    print(f"   🔍 Validating {filename}...")
    
    timestamps = []
    row_count = 0
    
    # Read all timestamps from the CSV
    try:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                unix_timestamp = int(row['unix_timestamp'])
                timestamps.append(unix_timestamp)
                row_count += 1
        
        print(f"   ✓ Successfully read {row_count:,} rows")
        
    except FileNotFoundError:
        print(f"   ❌ Error: File {filename} not found!")
        return False
    except Exception as e:
        print(f"   ❌ Error reading file: {e}")
        return False
    
    if not timestamps:
        print("   ❌ No data found in file!")
        return False
    
    # Sort timestamps to ensure proper order
    timestamps.sort()
    
    # Set expected range
    if expected_start is None:
        expected_start = timestamps[0]
    if expected_end is None:
        expected_end = timestamps[-1]
    
    # Get date range
    start_time = datetime.fromtimestamp(expected_start, timezone.utc)
    end_time = datetime.fromtimestamp(expected_end, timezone.utc)
    
    print(f"   📅 Expected range: {start_time} to {end_time}")
    
    # Calculate expected number of minutes
    expected_minutes = int((expected_end - expected_start) / 60) + 1
    print(f"   ⏱️  Expected candles: {expected_minutes:,}")
    print(f"   📈 Actual candles: {len(timestamps):,}")
    
    # Check for missing timestamps (should be every 60 seconds)
    missing_timestamps = []
    
    expected_timestamp = expected_start
    timestamp_set = set(timestamps)
    
    while expected_timestamp <= expected_end:
        if expected_timestamp not in timestamp_set:
            missing_timestamps.append(expected_timestamp)
        expected_timestamp += 60
    
    # Report results
    if not missing_timestamps:
        print("   ✅ SUCCESS: Data is complete with no missing timestamps!")
        return True
    else:
        print(f"   ❌ MISSING DATA: {len(missing_timestamps)} missing timestamps found!")
        return False

def fetch_year_data(year):
    """
    Fetch complete year data from Coinbase API
    """
    # For 2016, start from ETH launch date (May 23, 2016)
    if year == 2016:
        start_date = ETH_START_DATE
    else:
        start_date = datetime(year, 1, 1, tzinfo=timezone.utc)
    
    # For current year, only fetch up to current time
    current_time = datetime.now(timezone.utc)
    if year == current_time.year:
        # Set end date to current time, rounded down to the nearest minute
        end_date = current_time.replace(second=0, microsecond=0)
        print(f"   ⏰ Current year detected - fetching up to {end_date}")
    else:
        # For past years, fetch complete year
        end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    
    start_unix = int(start_date.timestamp())
    end_unix = int(end_date.timestamp())
    delta = GRANULARITY * MAX_CANDLES_PER_REQUEST  # 300 minutes = 5 hours
    
    print(f"📡 Fetching {year} data from Coinbase...")
    print(f"   Date range: {start_date} to {end_date}")
    
    # Generate all time intervals
    intervals = []
    current_start = start_unix
    interval_id = 0
    while current_start < end_unix:
        current_end = min(current_start + delta, end_unix)
        intervals.append((current_start, current_end, interval_id))
        current_start = current_end
        interval_id += 1
    
    print(f'   Total intervals: {len(intervals)}')
    print(f'   Using {MAX_WORKERS} workers')
    
    # Store results indexed by start timestamp for proper ordering
    results = {}
    completed_count = 0
    
    # Use ThreadPoolExecutor to fetch data in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_interval = {
            executor.submit(fetch_interval_data, start, end, iid): (start, end, iid)
            for start, end, iid in intervals
        }
        
        # Process completed tasks
        for future in as_completed(future_to_interval):
            start, end, iid = future_to_interval[future]
            try:
                csv_rows, start_ts, end_ts = future.result()
                if csv_rows is not None:
                    results[start_ts] = csv_rows
                completed_count += 1
                
                # Progress update
                if completed_count % 20 == 0 or completed_count == len(intervals):
                    print(f'   Progress: {completed_count}/{len(intervals)} intervals completed ({completed_count/len(intervals)*100:.1f}%)')
                    
            except Exception as e:
                print(f'   ✗ Exception in interval {iid}: {e}')
                completed_count += 1
    
    # Write results to CSV
    filename = f'ETHUSD_1m_candles_{year}.csv'
    print(f'   💾 Writing results to {filename}...')
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'open', 'close', 'volume', 'unix_timestamp', 'high', 'low']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        total_candles = 0
        for start_ts in sorted(results.keys()):
            csv_rows = results[start_ts]
            for row in csv_rows:
                writer.writerow(row)
                total_candles += 1
    
    print(f'   ✅ {year}: {total_candles:,} candles saved to {filename}')
    print(f'   ✅ {year}: Successfully fetched {len(results)}/{len(intervals)} intervals\n')
    
    return filename, total_candles

def validate_year_data(filename):
    """
    Validate year data using the integrated validation function
    """
    print(f"🔍 Validating {filename}...")
    try:
        is_valid = validate_data(filename)
        if is_valid:
            print(f"   ✅ {filename}: Validation PASSED - No missing data")
            return True, []
        else:
            print(f"   ❌ {filename}: Validation FAILED - Missing data detected")
            return False, []
            
    except Exception as e:
        print(f"   ❌ {filename}: Validation error: {e}")
        return False, []

def find_and_fetch_missing_data_integrated(filename):
    """
    Find missing data and fetch it using Binance (integrated version)
    """
    print(f"🔧 Processing missing data for {filename}...")
    
    # Step 1: Find missing data
    print("   📊 Analyzing missing timestamps...")
    missing_ranges = find_missing_timestamps(filename)
    
    if not missing_ranges:
        print("   ✅ No missing data found!")
        return True
    
    # Group consecutive missing timestamps
    all_missing_timestamps = []
    for start_ts, end_ts in missing_ranges:
        current_ts = start_ts
        while current_ts <= end_ts:
            all_missing_timestamps.append(current_ts)
            current_ts += 60
    
    grouped_ranges = group_consecutive_timestamps(all_missing_timestamps)
    
    if not grouped_ranges:
        print("   ✅ No missing data ranges to fetch!")
        return True
    
    print(f"   📋 Found {len(grouped_ranges)} missing ranges with {len(all_missing_timestamps):,} total missing timestamps")
    
    # Step 2: Fetch missing data from Binance
    print("   📡 Fetching missing data from Binance...")
    all_missing_data = []
    
    for i, (range_start, range_end) in enumerate(grouped_ranges):
        range_info = {
            'start_timestamp': range_start,
            'end_timestamp': range_end,
            'start_datetime': datetime.fromtimestamp(range_start, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'end_datetime': datetime.fromtimestamp(range_end, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'duration_minutes': (range_end - range_start) // 60 + 1
        }
        
        missing_rows, _ = fetch_missing_range(range_info, f"{os.path.basename(filename).replace('.csv', '')}-{i+1}")
        
        if missing_rows:
            all_missing_data.extend(missing_rows)
    
    # Step 3: Save missing data if any
    if all_missing_data:
        base_name = filename.replace('.csv', '')
        missing_data_file = f"{base_name}_missing.csv"
        
        with open(missing_data_file, 'w', newline='') as csvfile:
            fieldnames = ['timestamp', 'open', 'close', 'volume', 'unix_timestamp', 'high', 'low']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_missing_data)
        
        print(f"   💾 Saved {len(all_missing_data):,} missing candles to {missing_data_file}")
        
        # Step 4: Merge original with missing data
        complete_file = f"{base_name}_complete.csv"
        
        print("   🔄 Merging original and missing data...")
        merge_success = merge_csv_files(filename, missing_data_file, complete_file)
        
        if merge_success:
            # Step 5: Validate the complete file
            print("   🔍 Validating complete file...")
            is_valid = validate_data(complete_file)
            
            if is_valid:
                # Replace original file with complete file
                os.replace(complete_file, filename)
                print(f"   ✅ Successfully fixed all missing data in {filename}")
                
                # Clean up temporary files
                if os.path.exists(missing_data_file):
                    os.remove(missing_data_file)
                
                return True
            else:
                print(f"   ❌ Complete file still has issues")
                return False
        else:
            print(f"   ❌ Failed to merge data")
            return False
    else:
        print(f"   ⚠️  No missing data was retrieved")
        return False

def process_single_year(year):
    """
    Complete pipeline for processing a single year
    """
    print("="*70)
    print(f"🚀 PROCESSING YEAR {year}")
    print("="*70)
    
    # Step 1: Check if file already exists
    filename = f'ETHUSD_1m_candles_{year}.csv'
    if os.path.exists(filename):
        print(f"📄 {filename} already exists - validating...")
        is_valid = validate_data(filename)
        if is_valid:
            print(f"✅ {year}: Already complete and validated!")
            return True
        else:
            print(f"⚠️  {year}: File exists but has missing data - will fix...")
    else:
        # Step 2: Fetch year data
        try:
            filename, total_candles = fetch_year_data(year)
        except Exception as e:
            print(f"❌ {year}: Failed to fetch data: {e}")
            return False
    
    # Step 3: Validate the data
    is_valid = validate_data(filename)
    
    # Step 4: If validation failed, fix missing data
    if not is_valid:
        print(f"⚠️  {year}: Missing data detected - attempting to fix...")
        success = find_and_fetch_missing_data_integrated(filename)
        if not success:
            print(f"❌ {year}: Could not fix all missing data")
            return False
    
    print(f"🎉 {year}: COMPLETED SUCCESSFULLY!")
    return True

def main():
    """
    Main pipeline: Process years from 2016 to present for ETH
    """
    print("🚀 ETHEREUM DATA COLLECTION PIPELINE")
    print("="*70)
    print(f"📅 Processing years {STARTING_YEAR} to {CURRENT_YEAR}")
    print(f"🔧 Pipeline: Fetch → Validate → Fix Missing Data → Complete")
    print("="*70)
    
    successful_years = []
    failed_years = []
    
    for year in range(STARTING_YEAR, CURRENT_YEAR + 1):
        try:
            success = process_single_year(year)
            if success:
                successful_years.append(year)
            else:
                failed_years.append(year)
        except KeyboardInterrupt:
            print(f"\n⚠️  Pipeline interrupted by user at year {year}")
            break
        except Exception as e:
            print(f"❌ {year}: Unexpected error: {e}")
            failed_years.append(year)
    
    # Final summary
    print("\n" + "="*70)
    print("📊 PIPELINE SUMMARY")
    print("="*70)
    print(f"✅ Successful years: {len(successful_years)}")
    if successful_years:
        print(f"   {', '.join(map(str, successful_years))}")
    
    if failed_years:
        print(f"\n❌ Failed years: {len(failed_years)}")
        print(f"   {', '.join(map(str, failed_years))}")
        print("\n🔧 You can re-run the script to retry failed years")
    else:
        print(f"\n🎉 ALL YEARS COMPLETED SUCCESSFULLY!")
        print(f"📈 Complete Ethereum dataset from 2016 to {CURRENT_YEAR}")

if __name__ == "__main__":
    main()

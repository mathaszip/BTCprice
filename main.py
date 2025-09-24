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
BASE_URL = 'https://api.exchange.coinbase.com/products/BTC-USD/candles'
GRANULARITY = 60  # 1 minute in seconds
MAX_CANDLES_PER_REQUEST = 300  # Coinbase limit for 1m granularity
MAX_WORKERS = 30  # Number of parallel workers
RATE_LIMIT_DELAY = 0.05  # Delay between requests to avoid rate limits
MAX_RETRIES = 5  # Maximum number of retries for failed requests
RETRY_DELAY = 2  # Initial delay between retries (seconds)

# Pipeline configuration
STARTING_YEAR = 2025  # Start from 2019 since we have 2015-2018
CURRENT_YEAR = datetime.now().year

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

        print(f'✓ Fetched interval {interval_id}: {datetime.fromtimestamp(start_unix, timezone.utc)} to {datetime.fromtimestamp(end_unix, timezone.utc)} ({len(data)} candles)')
        return csv_rows, start_unix, end_unix
    
    except Exception as e:
        print(f'✗ Error fetching interval {interval_id}: {e}')
        return None, start_unix, end_unix

def fetch_year_data(year):
    """
    Fetch complete year data from Coinbase API
    """
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
    filename = f'BTCUSD_1m_candles_{year}.csv'
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
    Validate year data using the validation script
    """
    print(f"🔍 Validating {filename}...")
    try:
        result = subprocess.run(['python', 'validate_data.py', filename], 
                               capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print(f"   ✅ {filename}: Validation PASSED - No missing data")
            return True, []
        else:
            print(f"   ❌ {filename}: Validation FAILED - Missing data detected")
            return False, []
            
    except subprocess.TimeoutExpired:
        print(f"   ⚠️  {filename}: Validation timed out")
        return False, []
    except Exception as e:
        print(f"   ❌ {filename}: Validation error: {e}")
        return False, []

def find_and_fetch_missing_data(filename):
    """
    Find missing data and fetch it using Binance
    """
    print(f"🔧 Processing missing data for {filename}...")
    
    # Step 1: Find missing data
    print("   📊 Analyzing missing timestamps...")
    try:
        result = subprocess.run(['python', 'find_missing_data.py', filename], 
                               capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"   ❌ Failed to analyze missing data")
            return False
    except Exception as e:
        print(f"   ❌ Error analyzing missing data: {e}")
        return False
    
    # Step 2: Check if ranges file was created
    base_name = filename.replace('.csv', '')
    ranges_file = f"{base_name}_missing_ranges.json"
    
    if not os.path.exists(ranges_file):
        print(f"   ❌ Ranges file {ranges_file} not found")
        return False
    
    # Step 3: Fetch missing data using Binance
    print("   📡 Fetching missing data from Binance...")
    try:
        result = subprocess.run(['python', 'fetch_missing_data.py', ranges_file], 
                               capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"   ❌ Failed to fetch missing data from Binance")
            return False
    except Exception as e:
        print(f"   ❌ Error fetching missing data: {e}")
        return False
    
    # Step 4: Merge original with missing data
    missing_data_file = f"{base_name}_missing_data.csv"
    complete_file = f"{base_name}_complete.csv"
    
    print("   🔄 Merging original and missing data...")
    try:
        result = subprocess.run(['python', 'merge_csv_data.py', filename, missing_data_file, complete_file], 
                               capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"   ❌ Failed to merge data")
            return False
    except Exception as e:
        print(f"   ❌ Error merging data: {e}")
        return False
    
    # Step 5: Validate the complete file
    print("   🔍 Validating complete file...")
    is_valid, _ = validate_year_data(complete_file)
    
    if is_valid:
        # Replace original file with complete file
        os.replace(complete_file, filename)
        print(f"   ✅ Successfully fixed all missing data in {filename}")
        
        # Clean up temporary files
        for temp_file in [ranges_file, f"{base_name}_missing_timestamps.json", missing_data_file]:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        return True
    else:
        print(f"   ❌ Complete file still has issues")
        return False

def process_single_year(year):
    """
    Complete pipeline for processing a single year
    """
    print("="*70)
    print(f"🚀 PROCESSING YEAR {year}")
    print("="*70)
    
    # Step 1: Check if file already exists
    filename = f'BTCUSD_1m_candles_{year}.csv'
    if os.path.exists(filename):
        print(f"📄 {filename} already exists - validating...")
        is_valid, _ = validate_year_data(filename)
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
    is_valid, _ = validate_year_data(filename)
    
    # Step 4: If validation failed, fix missing data
    if not is_valid:
        print(f"⚠️  {year}: Missing data detected - attempting to fix...")
        success = find_and_fetch_missing_data(filename)
        if not success:
            print(f"❌ {year}: Could not fix all missing data")
            return False
    
    print(f"🎉 {year}: COMPLETED SUCCESSFULLY!")
    return True

def main():
    """
    Main pipeline: Process years from 2019 to present
    """
    print("🚀 BITCOIN DATA COLLECTION PIPELINE")
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
        print(f"📈 Complete Bitcoin dataset from 2015 to {CURRENT_YEAR}")

if __name__ == "__main__":
    main()

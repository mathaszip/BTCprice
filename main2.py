import requests
import time
import os
from datetime import datetime, timedelta, timezone
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Bitstamp API configuration
BASE_URL = 'https://www.bitstamp.net/api/v2/ohlc/btcusd'
STEP = 60  # 1 minute in seconds
MAX_CANDLES_PER_REQUEST = 1000  # Bitstamp limit for 1m granularity
MAX_WORKERS = 50  # Number of parallel workers
RATE_LIMIT_DELAY = 0.05  # Delay between requests to avoid rate limits
MAX_RETRIES = 5  # Maximum number of retries for failed requests
RETRY_DELAY = 2  # Initial delay between retries (seconds)

# Data collection configuration
START_DATE = datetime(2011, 8, 18, tzinfo=timezone.utc)  # Earliest Bitstamp data
END_DATE = datetime(2015, 7, 21, 0, 0, 0, tzinfo=timezone.utc)  # Target end date

def get_candles_bitstamp(start_unix, end_unix, retry_count=0):
    """
    Fetch candle data from Bitstamp API with robust retry logic
    """
    params = {
        'start': start_unix,
        'end': end_unix,
        'step': STEP,
        'limit': MAX_CANDLES_PER_REQUEST
    }

    try:
        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'ohlc' in data['data'] and data['data']['ohlc']:
                # Convert Bitstamp format to our database format
                converted_data = []
                for candle in data['data']['ohlc']:
                    timestamp_unix = int(candle['timestamp'])
                    timestamp = datetime.fromtimestamp(timestamp_unix, timezone.utc)
                    open_price = float(candle['open'])
                    high_price = float(candle['high'])
                    low_price = float(candle['low'])
                    close_price = float(candle['close'])
                    volume = float(candle['volume'])

                    converted_data.append((timestamp, open_price, high_price, low_price, close_price, volume))

                return converted_data
            else:
                return None

        elif response.status_code == 429:  # Rate limit exceeded
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY * (2 ** retry_count)
                print(f'   ⏳ Bitstamp rate limit hit. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
                time.sleep(retry_delay)
                return get_candles_bitstamp(start_unix, end_unix, retry_count + 1)
            else:
                print(f'   ❌ Bitstamp rate limit exceeded. Max retries reached.')
                return None
        else:
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY * (2 ** retry_count)
                print(f'   ⚠️  Bitstamp request failed: {response.status_code}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
                time.sleep(retry_delay)
                return get_candles_bitstamp(start_unix, end_unix, retry_count + 1)
            else:
                print(f'   ❌ Bitstamp request failed: {response.status_code}. Max retries reached.')
                return None

    except Exception as e:
        if retry_count < MAX_RETRIES:
            retry_delay = RETRY_DELAY * (2 ** retry_count)
            print(f'   🔄 Bitstamp request exception: {e}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
            time.sleep(retry_delay)
            return get_candles_bitstamp(start_unix, end_unix, retry_count + 1)
        else:
            print(f'   ❌ Bitstamp request exception: {e}. Max retries reached.')
            return None

def fetch_interval_data(start_unix, end_unix, interval_id):
    """
    Fetch data for a specific time interval from Bitstamp API
    """
    try:
        raw_data = get_candles_bitstamp(start_unix, end_unix)
        if raw_data is None:
            print(f'✗ Error fetching interval {interval_id}: No data returned')
            return None

        # Convert to CSV format
        csv_rows = []
        for candle in raw_data:
            timestamp, open_price, high_price, low_price, close_price, volume = candle
            timestamp_iso = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            csv_rows.append({
                'timestamp': timestamp_iso,
                'open': open_price,
                'close': close_price,
                'volume': volume,
                'unix_timestamp': int(timestamp.timestamp()),
                'high': high_price,
                'low': low_price
            })

        print(f'✓ Fetched interval {interval_id}: {datetime.fromtimestamp(start_unix, timezone.utc)} to {datetime.fromtimestamp(end_unix, timezone.utc)} ({len(csv_rows)} candles)')
        return csv_rows, start_unix, end_unix

    except Exception as e:
        print(f'✗ Error fetching interval {interval_id}: {e}')
        return None, start_unix, end_unix

def fetch_year_data(year_start, year_end, year):
    """
    Fetch complete year data from Bitstamp API and save to CSV
    """
    start_unix = int(year_start.timestamp())
    end_unix = int(year_end.timestamp())
    delta = STEP * MAX_CANDLES_PER_REQUEST  # 1000 minutes = ~17 hours

    print(f"📡 Fetching {year} data from Bitstamp...")
    print(f"   Date range: {year_start} to {year_end}")

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

def main():
    """
    Main function to fetch Bitstamp BTC/USD data from 2011-08-18 to 2015-07-21
    """
    print("🚀 BITSTAMP BTC/USD DATA COLLECTION")
    print("="*70)
    print(f"📅 Date range: {START_DATE} to {END_DATE}")
    print(f"🔧 Workers: {MAX_WORKERS}")
    print(f"⏳ Retry delay: {RETRY_DELAY}s")
    print(f"🔄 Max retries: {MAX_RETRIES}")
    print("="*70)

    # Calculate years to process
    start_year = START_DATE.year
    end_year = END_DATE.year

    successful_years = []
    failed_years = []

    for year in range(start_year, end_year + 1):
        try:
            print(f"📊 Processing year {year}...")

            # Determine year start and end
            year_start = max(START_DATE, datetime(year, 1, 1, tzinfo=timezone.utc))
            year_end = min(END_DATE, datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc))

            # Skip if year is completely before start date
            if year_end < START_DATE:
                continue

            # Skip if year is completely after end date
            if year_start > END_DATE:
                continue

            print(f"   📅 Year range: {year_start} to {year_end}")

            # Fetch data for this year
            filename, total_candles = fetch_year_data(year_start, year_end, year)

            if total_candles > 0:
                successful_years.append(year)
                print(f"   ✅ {year}: {total_candles:,} candles saved to {filename}")
            else:
                failed_years.append(year)
                print(f"   ❌ {year}: Failed to fetch any data")

        except KeyboardInterrupt:
            print(f"\n⚠️  Pipeline interrupted by user at year {year}")
            break
        except Exception as e:
            print(f"❌ {year}: Unexpected error: {e}")
            failed_years.append(year)

    # Final summary
    print("\n" + "="*70)
    print("📊 BITSTAMP DATA COLLECTION SUMMARY")
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
        print(f"� Check the generated CSV files for your Bitstamp BTC/USD data (2011-2015)")

if __name__ == "__main__":
    main()
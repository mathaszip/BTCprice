import requests
import time
import os
from datetime import datetime, timedelta, timezone
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Binance API configuration
BASE_URL = 'https://api.binance.com/api/v3/klines'
STEP = 1  # 1 second in seconds
INTERVAL = '1s'  # 1 second
MAX_CANDLES_PER_REQUEST = 1000  # Binance limit for 1s
MAX_WORKERS = 50  # Number of parallel workers
RATE_LIMIT_DELAY = 0.025  # Delay between requests to avoid rate limits
MAX_RETRIES = 5  # Maximum number of retries for failed requests
RETRY_DELAY = 2  # Initial delay between retries (seconds)

# Data collection configuration
START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)  # Start from 2024
END_DATE = datetime(2025, 9, 24, tzinfo=timezone.utc)  # Up to current date

def get_candles_binance(start_unix, end_unix, retry_count=0):
    """
    Fetch candle data from Binance API with robust retry logic
    """
    params = {
        'symbol': 'BTCUSDT',
        'interval': INTERVAL,
        'startTime': start_unix * 1000,  # Binance uses milliseconds
        'endTime': end_unix * 1000,
        'limit': MAX_CANDLES_PER_REQUEST
    }

    try:
        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list):
                # Convert Binance format to our database format
                # Binance: [open_time, open, high, low, close, volume, close_time, ...]
                converted_data = []
                for candle in data:
                    timestamp_unix = int(candle[0] / 1000)  # Convert to seconds
                    timestamp = datetime.fromtimestamp(timestamp_unix, timezone.utc)
                    open_price = float(candle[1])
                    high_price = float(candle[2])
                    low_price = float(candle[3])
                    close_price = float(candle[4])
                    volume = float(candle[5])

                    converted_data.append((timestamp, open_price, high_price, low_price, close_price, volume))

                return converted_data
            else:
                return None

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

def fetch_interval_data(start_unix, end_unix, interval_id):
    """
    Fetch data for a specific time interval from Binance API
    """
    try:
        raw_data = get_candles_binance(start_unix, end_unix)
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

def fetch_day_data(day_start, day_end, day_str):
    """
    Fetch complete day data from Coinbase Pro API and save to CSV
    """
    start_unix = int(day_start.timestamp())
    end_unix = int(day_end.timestamp())
    delta = STEP * MAX_CANDLES_PER_REQUEST  # 1000 seconds = ~16.7 minutes

    print(f"📡 Fetching {day_str} data from Coinbase...")
    print(f"   Date range: {day_start} to {day_end}")

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

    # Create directory if not exists
    os.makedirs('data/btc/1sec', exist_ok=True)

    # Write results to CSV
    filename = f'data/btc/1sec/BTCUSD_1s_candles_{day_str}.csv'
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

    print(f'   ✅ {day_str}: {total_candles:,} candles saved to {filename}')
    print(f'   ✅ {day_str}: Successfully fetched {len(results)}/{len(intervals)} intervals\n')

    return filename, total_candles

def main():
    """
    Main function to fetch Coinbase Pro BTC/USD 1-second data from 2024-01-01 to 2025-09-24
    """
    print("🚀 BINANCE BTC/USDT 1-SECOND DATA COLLECTION")
    print("="*70)
    print(f"📅 Date range: {START_DATE} to {END_DATE}")
    print(f"🔧 Workers: {MAX_WORKERS}")
    print(f"⏳ Retry delay: {RETRY_DELAY}s")
    print(f"🔄 Max retries: {MAX_RETRIES}")
    print("="*70)

    # Calculate days to process
    current_date = START_DATE
    successful_days = []
    failed_days = []

    while current_date <= END_DATE:
        try:
            day_str = current_date.strftime('%Y-%m-%d')
            print(f"📊 Processing day {day_str}...")

            # Day start and end
            day_start = current_date
            day_end = min(current_date + timedelta(days=1), END_DATE + timedelta(days=1))

            print(f"   📅 Day range: {day_start} to {day_end}")

            # Fetch data for this day
            filename, total_candles = fetch_day_data(day_start, day_end, day_str)

            if total_candles > 0:
                successful_days.append(day_str)
                print(f"   ✅ {day_str}: {total_candles:,} candles saved to {filename}")
            else:
                failed_days.append(day_str)
                print(f"   ❌ {day_str}: Failed to fetch any data")

        except KeyboardInterrupt:
            print(f"\n⚠️  Pipeline interrupted by user at day {day_str}")
            break
        except Exception as e:
            print(f"❌ {day_str}: Unexpected error: {e}")
            failed_days.append(day_str)

        current_date += timedelta(days=1)

    # Final summary
    print("\n" + "="*70)
    print("📊 BINANCE 1-SECOND DATA COLLECTION SUMMARY")
    print("="*70)
    print(f"✅ Successful days: {len(successful_days)}")
    if successful_days:
        print(f"   First: {successful_days[0]}, Last: {successful_days[-1]}")

    if failed_days:
        print(f"\n❌ Failed days: {len(failed_days)}")
        print(f"   {', '.join(failed_days[:10])}{'...' if len(failed_days) > 10 else ''}")
        print("\n🔧 You can re-run the script to retry failed days")
    else:
        print(f"\n🎉 ALL DAYS COMPLETED SUCCESSFULLY!")
        print(f"📁 Check the generated CSV files in /data/btc/1sec/ for your Binance BTC/USDT 1-second data (2024-2025)")

if __name__ == "__main__":
    main()
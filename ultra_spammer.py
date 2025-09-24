import requests
import time
import os
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'binance_data')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'kurwa')

# API configuration
BASE_URL = 'https://api.binance.com/api/v3/klines'
SYMBOL = 'BTCUSDT'
INTERVAL = '1s'
LIMIT = 1000  # Max allowed per request

# Parallel processing configuration (same as main.py)
MAX_WORKERS = 50  # Increased from 30 to 50 as requested
RATE_LIMIT_DELAY = 0.05  # Delay between requests to avoid rate limits
MAX_RETRIES = 5  # Maximum number of retries for failed requests
RETRY_DELAY = 2  # Initial delay between retries (seconds)

# Slack notification configuration
SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'

start_dt = datetime(2025, 3, 22, 11, 1, 20)
end_dt = datetime(2025, 9, 24, 19, 0, 0)

start_ms = int(start_dt.timestamp() * 1000)
end_ms = int(end_dt.timestamp() * 1000)
step = LIMIT * 1000  # 1000 seconds (ms) per request

def fetch_interval_data(start_ms, end_ms, interval_id, retry_count=0):
    """
    Fetch data for a specific time interval from Binance API
    """
    try:
        period_end = min(start_ms + step - 1, end_ms)
        params = {
            'symbol': SYMBOL,
            'interval': INTERVAL,
            'startTime': start_ms,
            'endTime': period_end,
            'limit': LIMIT
        }

        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            klines = response.json()
            if not klines:
                print(f'No data for interval {interval_id}: {datetime.utcfromtimestamp(start_ms/1000)}')
                return [], start_ms, end_ms

            # Convert to database format
            batch_data = []
            for entry in klines:
                open_time = int(entry[0]) // 1000
                timestamp = datetime.utcfromtimestamp(open_time)
                open_price = float(entry[1])
                high_price = float(entry[2])
                low_price = float(entry[3])
                close_price = float(entry[4])
                volume = float(entry[5])
                batch_data.append((timestamp, open_price, high_price, low_price, close_price, volume))

            print(f'✓ Fetched interval {interval_id}: {datetime.utcfromtimestamp(start_ms/1000)} ({len(batch_data)} records)')
            return batch_data, start_ms, end_ms

        elif response.status_code == 429:  # Rate limit exceeded
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY * (2 ** retry_count)  # Exponential backoff
                print(f'Rate limit hit on interval {interval_id}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
                time.sleep(retry_delay)
                return fetch_interval_data(start_ms, end_ms, interval_id, retry_count + 1)
            else:
                error_msg = f'🚨 CRITICAL: Rate limit exceeded on interval {interval_id} after {MAX_RETRIES} retries. Time range: {datetime.utcfromtimestamp(start_ms/1000)} to {datetime.utcfromtimestamp(end_ms/1000)}'
                print(error_msg)
                send_slack_notification(error_msg)
                return [], start_ms, end_ms
        else:
            # Other HTTP errors
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY * (2 ** retry_count)
                print(f'HTTP {response.status_code} on interval {interval_id}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
                time.sleep(retry_delay)
                return fetch_interval_data(start_ms, end_ms, interval_id, retry_count + 1)
            else:
                error_msg = f'🚨 CRITICAL: HTTP {response.status_code} error on interval {interval_id} after {MAX_RETRIES} retries. Time range: {datetime.utcfromtimestamp(start_ms/1000)} to {datetime.utcfromtimestamp(end_ms/1000)}'
                print(error_msg)
                send_slack_notification(error_msg)
                return [], start_ms, end_ms

    except Exception as e:
        if retry_count < MAX_RETRIES:
            retry_delay = RETRY_DELAY * (2 ** retry_count)
            print(f'Exception on interval {interval_id}: {e}. Retrying in {retry_delay}s... (attempt {retry_count + 1}/{MAX_RETRIES})')
            time.sleep(retry_delay)
            return fetch_interval_data(start_ms, end_ms, interval_id, retry_count + 1)
        else:
            error_msg = f'🚨 CRITICAL: Exception on interval {interval_id} after {MAX_RETRIES} retries: {str(e)}. Time range: {datetime.utcfromtimestamp(start_ms/1000)} to {datetime.utcfromtimestamp(end_ms/1000)}'
            print(error_msg)
            send_slack_notification(error_msg)
            return [], start_ms, end_ms

def send_slack_notification(message):
    """Send notification to Slack webhook"""
    payload = {"text": message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            print(f"Failed to send Slack notification: {response.status_code}")
    except Exception as e:
        print(f"Error sending Slack notification: {e}")

def create_database_and_table():
    """Create database and table if they don't exist"""
    try:
        # Connect to PostgreSQL server (not specific database)
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='postgres'  # Connect to default postgres database first
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create database if it doesn't exist
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"Created database: {DB_NAME}")

        cursor.close()
        conn.close()

        # Now connect to the specific database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS btc_price_data (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                open_price DECIMAL(20, 8) NOT NULL,
                                high_price DECIMAL(20, 8) NOT NULL,
                low_price DECIMAL(20, 8) NOT NULL,
                close_price DECIMAL(20, 8) NOT NULL,
                volume DECIMAL(20, 8) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp)
            )
        """)

        # Create index on timestamp for better query performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_btc_timestamp
            ON btc_price_data(timestamp)
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Database and table setup completed")

    except Exception as e:
        print(f"Error setting up database: {e}")
        raise

def insert_data_to_db(data_batch):
    """Insert data batch into PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Use execute_batch for better performance
        insert_query = """
            INSERT INTO btc_price_data (timestamp, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING
        """

        execute_batch(cursor, insert_query, data_batch, page_size=1000)
        conn.commit()

        cursor.close()
        conn.close()

        return True
    except Exception as e:
        print(f"Error inserting data to database: {e}")
        return False

# Initialize database
create_database_and_table()

print("🚀 ULTRA FAST BINANCE DATA FETCHER")
print("="*60)
print(f"📅 Date range: {start_dt} to {end_dt}")
print(f"🔧 Workers: {MAX_WORKERS}")
print(f"⏳ Rate limit delay: {RATE_LIMIT_DELAY}s")
print(f"🔄 Max retries: {MAX_RETRIES}")
print(f"📊 Symbol: {SYMBOL}, Interval: {INTERVAL}")
print("="*60)

# Generate all time intervals
intervals = []
current_ms = start_ms
interval_id = 0
while current_ms <= end_ms:
    period_end = min(current_ms + step - 1, end_ms)
    intervals.append((current_ms, period_end, interval_id))
    current_ms = period_end + 1000  # Move to next second
    interval_id += 1

print(f'Total intervals to process: {len(intervals)}')
print(f'Using {MAX_WORKERS} parallel workers')

# Store results for ordered processing
results = {}
completed_count = 0
lock = threading.Lock()  # For thread-safe database operations

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
            batch_data, start_ts, end_ts = future.result()
            if batch_data:
                # Insert data to database immediately (thread-safe)
                with lock:
                    success = insert_data_to_db(batch_data)
                    if success:
                        print(f'💾 Stored interval {iid}: {datetime.utcfromtimestamp(start_ts/1000)} ({len(batch_data)} records)')
                    else:
                        print(f'❌ Failed to store interval {iid}')

            completed_count += 1

            # Progress update
            if completed_count % 50 == 0 or completed_count == len(intervals):
                print(f'📈 Progress: {completed_count}/{len(intervals)} intervals completed ({completed_count/len(intervals)*100:.1f}%)')

        except Exception as e:
            print(f'❌ Exception in interval {iid}: {e}')
            completed_count += 1

print("\n🎉 ALL INTERVALS PROCESSED!")
print("📁 Data saved to PostgreSQL database")
import requests
from datetime import datetime, timedelta
import time

def check_bitstamp_data_availability():
    """
    Check how far back Bitstamp ETH/USD data goes using daily candles
    """
    currency_pair = 'ethusd'
    url = f'https://www.bitstamp.net/api/v2/ohlc/{currency_pair}/'

    # Start from a date we know has data (2016-05-24) and work backwards
    current_date = datetime(2016, 5, 24)
    earliest_found = None
    latest_found = None

    print("🔍 Checking Bitstamp ETH/USD data availability...")
    print("📊 Using daily candles to find date range\n")

    # First, let's check recent data to see the latest available
    print("📅 Checking most recent data...")
    recent_start = int(datetime.now().timestamp()) - (365 * 24 * 60 * 60)  # 1 year ago
    recent_end = int(datetime.now().timestamp())

    params = {
        'step': 86400,      # 1 day candles
        'limit': 1,         # Just get the most recent
        'start': recent_start,
        'end': recent_end
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'ohlc' in data['data'] and data['data']['ohlc']:
                latest_candle = data['data']['ohlc'][0]
                latest_timestamp = int(latest_candle['timestamp'])
                latest_found = datetime.fromtimestamp(latest_timestamp)
                print(f"✅ Latest data available: {latest_found.strftime('%Y-%m-%d')}")
            else:
                print("❌ No recent data found")
                return
        else:
            print(f"❌ API error: {response.status_code}")
            return
    except Exception as e:
        print(f"❌ Error checking recent data: {e}")
        return

    # Now work backwards from today to find the earliest data
    print("\n📅 Checking historical data availability...")
    print("🔄 Working backwards day by day from today...")

    # Start from today
    test_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    earliest_found = None

    while True:  # Keep going until we can't find data
        start_timestamp = int(test_date.timestamp())
        end_timestamp = int((test_date + timedelta(days=1)).timestamp())

        print(f"Timestamps: start={start_timestamp}, end={end_timestamp}")  # Debug

        params = {
            'step': 86400,      # 1 day candles
            'limit': 1,         # Just get 1 candle
            'start': start_timestamp,
            'end': end_timestamp
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            print(f"📅 Checking {test_date.strftime('%Y-%m-%d')}... ", end="")

            if response.status_code == 200:
                data = response.json()
                print(f"Response: {data}")  # Debug: print the full response
                if 'data' in data and 'ohlc' in data['data'] and data['data']['ohlc']:
                    # Found data for this date
                    candle = data['data']['ohlc'][0]
                    timestamp = int(candle['timestamp'])
                    candle_date = datetime.fromtimestamp(timestamp)
                    price = float(candle['open'])

                    print(f"✅ Found! Price: ${price:.2f}")

                    if earliest_found is None or candle_date < earliest_found:
                        earliest_found = candle_date

                    # Continue to previous day
                    test_date = test_date - timedelta(days=1)
                else:
                    # No data for this date
                    print("❌ No data")
                    break
            else:
                print(f"❌ API error: {response.status_code}")
                break

        except Exception as e:
            print(f"❌ Error: {e}")
            break

        # Add small delay to be respectful to the API
        time.sleep(0.1)

    # Print final results
    print("\n" + "="*60)
    print("📊 BITSTAMP ETH/USD DATA AVAILABILITY RESULTS")
    print("="*60)

    if earliest_found and latest_found:
        days_available = (latest_found - earliest_found).days
        print(f"📅 Date Range: {earliest_found.strftime('%Y-%m-%d')} to {latest_found.strftime('%Y-%m-%d')}")
        print(f"📈 Total Days: {days_available:,}")
        print(f"💰 Early Price: ~$? (from {earliest_found.strftime('%Y-%m-%d')})")  # Based on sample data
    else:
        print("❌ Could not determine data availability")

if __name__ == "__main__":
    check_bitstamp_data_availability()

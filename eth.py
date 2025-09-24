import requests
from datetime import datetime, timedelta, timezone

# Coinbase API configuration for ETH-USD
BASE_URL = 'https://api.exchange.coinbase.com/products/ETH-USD/candles'
GRANULARITY = 86400  # 1 day in seconds

def check_date_has_data(target_date):
    """
    Check if ETH data exists for a specific date by requesting 1-day candle
    """
    # Set start and end to the same day (target_date 00:00 to 23:59)
    start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1)
    
    params = {
        'start': start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'end': end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'granularity': GRANULARITY
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            return len(data) > 0  # True if we got candle data
        else:
            print(f"API Error for {target_date.strftime('%Y-%m-%d')}: {response.status_code}")
            return False
    except Exception as e:
        print(f"Request failed for {target_date.strftime('%Y-%m-%d')}: {e}")
        return False

def find_first_eth_date():
    """
    Find the first date ETH had trading data on Coinbase by going backwards from 2016
    """
    print("🔍 Finding first date ETH had data on Coinbase...")
    
    # Start from January 1, 2016
    current_date = datetime(2017, 1, 1, tzinfo=timezone.utc)
    
    days_checked = 0
    
    while True:
        has_data = check_date_has_data(current_date)
        
        if has_data:
            print(f"✅ {current_date.strftime('%Y-%m-%d')}: Has data")
            current_date -= timedelta(days=1)
            days_checked += 1
        else:
            # No data found - this is the first date without data
            # So the previous day was the last with data
            first_date = current_date + timedelta(days=1)
            print(f"\n🎯 First ETH data date found: {first_date.strftime('%Y-%m-%d')}")
            print(f"   Checked {days_checked} days backwards")
            return first_date
        
        # Safety check to prevent infinite loops (though unlikely)
        if days_checked > 365 * 10:  # 10 years
            print("⚠️  Safety limit reached - stopping search")
            return None

if __name__ == "__main__":
    first_date = find_first_eth_date()
    if first_date:
        print(f"\n📅 ETH first appeared on Coinbase on: {first_date.strftime('%B %d, %Y')}")
    else:
        print("\n❌ Could not determine first ETH date")
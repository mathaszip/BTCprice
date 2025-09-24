import csv
import os
from datetime import datetime, timedelta
import sys

def validate_btc_dataset():
    """Comprehensive validation of BTC dataset from 2011-2025"""

    data_dir = 'data/btc'
    expected_start = {
        'timestamp': '2011-08-18 12:37:00',
        'unix': 1313671020,
        'open': 10.9,
        'close': 10.9,
        'volume': 0.48990826,
        'high': 10.9,
        'low': 10.9
    }

    expected_end = {
        'timestamp': '2025-09-24 15:29:00',
        'unix': 1758727740,
        'open': 113682.02,
        'close': 113700.11,
        'volume': 3.46633664,
        'high': 113714,
        'low': 113679.09
    }

    print("🔍 VALIDATING BTC DATASET (2011-2025)")
    print("=" * 50)

    # Check all years exist
    print("📁 Checking file existence...")
    missing_years = []
    for year in range(2011, 2026):
        filename = f'BTCUSD_1m_candles_{year}.csv'
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            missing_years.append(year)
        else:
            print(f"✅ {year}: {filename}")

    if missing_years:
        print(f"❌ Missing years: {missing_years}")
        return False
    else:
        print("✅ All years present (2011-2025)")

    print("\n📊 Analyzing data continuity and format...")

    all_data = []
    total_candles = 0
    prev_timestamp = None
    prev_year_end = None

    for year in range(2011, 2026):
        filename = f'BTCUSD_1m_candles_{year}.csv'
        filepath = os.path.join(data_dir, filename)

        try:
            with open(filepath, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)

                # Validate header
                expected_header = ['timestamp', 'open', 'close', 'volume', 'unix_timestamp', 'high', 'low']
                if header != expected_header:
                    print(f"❌ {year}: Invalid header format")
                    return False

                year_data = []
                timestamps = set()

                for row_num, row in enumerate(reader, 1):
                    if len(row) != 7:
                        print(f"❌ {year}: Row {row_num} has {len(row)} columns, expected 7")
                        return False

                    try:
                        # Parse data
                        timestamp_str, open_price, close_price, volume, unix_ts, high_price, low_price = row

                        # Convert to appropriate types
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        open_price = float(open_price)
                        close_price = float(close_price)
                        volume = float(volume)
                        unix_ts = int(unix_ts)
                        high_price = float(high_price)
                        low_price = float(low_price)

                        # Validate data ranges
                        if open_price <= 0 or close_price <= 0 or high_price <= 0 or low_price <= 0:
                            print(f"❌ {year}: Invalid price data at row {row_num}")
                            return False

                        if high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
                            print(f"❌ {year}: Invalid OHLC relationship at row {row_num}")
                            return False

                        # Check for duplicate timestamps within year
                        if unix_ts in timestamps:
                            print(f"❌ {year}: Duplicate timestamp {unix_ts} at row {row_num}")
                            return False
                        timestamps.add(unix_ts)

                        year_data.append({
                            'timestamp': timestamp,
                            'unix': unix_ts,
                            'open': open_price,
                            'close': close_price,
                            'volume': volume,
                            'high': high_price,
                            'low': low_price
                        })

                    except (ValueError, IndexError) as e:
                        print(f"❌ {year}: Parse error at row {row_num}: {e}")
                        return False

                if not year_data:
                    print(f"❌ {year}: No data found")
                    return False

                # Check year boundaries
                year_start = year_data[0]
                year_end = year_data[-1]

                print(f"📅 {year}: {len(year_data):,} candles | {year_start['timestamp']} → {year_end['timestamp']}")

                # Validate start/end points
                if year == 2011:
                    if (year_start['timestamp'].strftime('%Y-%m-%d %H:%M:%S') != expected_start['timestamp'] or
                        abs(year_start['open'] - expected_start['open']) > 0.01):
                        print(f"❌ 2011 start mismatch!")
                        print(f"   Expected: {expected_start['timestamp']}")
                        print(f"   Found:    {year_start['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                        return False
                    print("✅ 2011 start validated")

                if year == 2025:
                    if (year_end['timestamp'].strftime('%Y-%m-%d %H:%M:%S') != expected_end['timestamp'] or
                        abs(year_end['close'] - expected_end['close']) > 0.01):
                        print(f"❌ 2025 end mismatch!")
                        print(f"   Expected: {expected_end['timestamp']}")
                        print(f"   Found:    {year_end['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                        return False
                    print("✅ 2025 end validated")

                # Check continuity between years (allow small gaps at year boundaries)
                if prev_year_end:
                    expected_next = prev_year_end['timestamp'] + timedelta(minutes=1)
                    actual_gap = year_start['timestamp'] - expected_next

                    if actual_gap > timedelta(minutes=5):  # Allow up to 5 minute gaps
                        print(f"❌ Large gap between {year-1} and {year}!")
                        print(f"   {year-1} ends: {prev_year_end['timestamp']}")
                        print(f"   {year} starts: {year_start['timestamp']}")
                        print(f"   Gap: {actual_gap}")
                        return False
                    elif actual_gap > timedelta(minutes=0):
                        print(f"⚠️  Small gap between {year-1} and {year}: {actual_gap} (acceptable)")

                prev_year_end = year_end
                all_data.extend(year_data)
                total_candles += len(year_data)

        except Exception as e:
            print(f"❌ Error reading {year}: {e}")
            return False

    print("\n📈 SUMMARY")
    print(f"📊 Total candles: {total_candles:,}")
    print(f"📅 Date range: {all_data[0]['timestamp']} → {all_data[-1]['timestamp']}")

    # Final continuity check (allow small gaps at year boundaries)
    print("\n🔗 Checking overall continuity...")
    gaps = 0
    acceptable_gap_minutes = 5  # Allow up to 5 minute gaps

    for i in range(1, len(all_data)):
        expected_ts = all_data[i-1]['timestamp'] + timedelta(minutes=1)
        actual_ts = all_data[i]['timestamp']
        gap = actual_ts - expected_ts

        # Check if this is a year boundary gap (small and at year start)
        is_year_boundary = (actual_ts.day == 1 and actual_ts.month == 1 and actual_ts.hour == 0 and
                           actual_ts.minute <= acceptable_gap_minutes)

        if actual_ts != expected_ts:
            if gap <= timedelta(minutes=acceptable_gap_minutes) and is_year_boundary:
                print(f"⚠️  Acceptable year-boundary gap: {all_data[i-1]['timestamp']} → {actual_ts} ({gap})")
            else:
                gaps += 1
                if gaps <= 3:  # Show first few problematic gaps
                    print(f"❌ Gap at {all_data[i-1]['timestamp']} → {actual_ts} ({gap})")

    if gaps == 0:
        print("✅ No problematic gaps found - data continuity is good!")
    else:
        print(f"❌ Found {gaps} problematic gaps in continuity")
        return False

    print("\n🎉 VALIDATION COMPLETE - ALL CHECKS PASSED!")
    print("✅ Dataset is complete and continuous from 2011-08-18 to 2025-09-24")
    return True

if __name__ == "__main__":
    success = validate_btc_dataset()
    sys.exit(0 if success else 1)
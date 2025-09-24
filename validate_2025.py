import csv
from datetime import datetime, timezone
import sys

def validate_2025_data(filename, target_end_time):
    """
    Validate 2025 Bitcoin data up to a specific end time
    """
    print(f"🔍 Validating 2025 data in {filename} up to {target_end_time}...")
    
    timestamps = []
    row_count = 0
    
    # Target end timestamp
    target_end_timestamp = int(target_end_time.timestamp())
    
    # Read all timestamps from the CSV
    try:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                unix_timestamp = int(row['unix_timestamp'])
                # Only include timestamps up to our target end time
                if unix_timestamp <= target_end_timestamp:
                    timestamps.append(unix_timestamp)
                row_count += 1
        
        print(f"✓ Read {row_count:,} total rows from {filename}")
        print(f"✓ Found {len(timestamps):,} rows within target range")
        
    except FileNotFoundError:
        print(f"❌ Error: File {filename} not found!")
        return False
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False
    
    if not timestamps:
        print("❌ No data found in target range!")
        return False
    
    # Sort timestamps to ensure proper order
    timestamps.sort()
    
    # Get actual date range
    start_time = datetime.fromtimestamp(timestamps[0], timezone.utc)
    end_time = datetime.fromtimestamp(timestamps[-1], timezone.utc)
    
    print(f"📅 Actual data range: {start_time} to {end_time}")
    print(f"📊 Target end time: {target_end_time}")
    
    # Calculate expected number of minutes
    expected_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    expected_start_timestamp = int(expected_start.timestamp())
    
    expected_minutes = int((target_end_timestamp - expected_start_timestamp) / 60) + 1
    print(f"⏱️  Expected candles: {expected_minutes:,}")
    print(f"📈 Actual candles: {len(timestamps):,}")
    
    # Check for missing timestamps (should be every 60 seconds)
    missing_timestamps = []
    
    expected_timestamp = expected_start_timestamp
    timestamp_set = set(timestamps)
    
    while expected_timestamp <= target_end_timestamp:
        if expected_timestamp not in timestamp_set:
            missing_timestamps.append(expected_timestamp)
        expected_timestamp += 60
    
    # Check if we have the exact target end time
    has_target_end = target_end_timestamp in timestamp_set
    
    # Report results
    print("\n" + "="*60)
    print("VALIDATION RESULTS")
    print("="*60)
    
    if not missing_timestamps and has_target_end:
        print("✅ SUCCESS: 2025 data is complete up to target time!")
        print(f"✅ All {len(timestamps):,} candles are present and accounted for")
        print(f"✅ Data correctly ends at {target_end_time}")
        return True
    else:
        issues = []
        
        if missing_timestamps:
            print(f"❌ MISSING DATA: {len(missing_timestamps)} missing timestamps found!")
            issues.append(f"{len(missing_timestamps)} missing timestamps")
            
            print("\nFirst 10 missing timestamps:")
            for i, ts in enumerate(missing_timestamps[:10]):
                missing_time = datetime.fromtimestamp(ts, timezone.utc)
                print(f"  {i+1}. {missing_time} (timestamp: {ts})")
            
            if len(missing_timestamps) > 10:
                print(f"  ... and {len(missing_timestamps) - 10} more")
        
        if not has_target_end:
            print(f"⚠️  TARGET END TIME: Data does not reach {target_end_time}")
            print(f"   Last timestamp: {end_time}")
            issues.append("missing target end time")
        
        print(f"\n💡 Summary: {', '.join(issues)}")
        return False

def main():
    filename = 'BTCUSD_1m_candles_2025.csv'
    target_end_time = datetime(2025, 9, 24, 15, 0, tzinfo=timezone.utc)  # Sep 24, 2025 15:00 UTC
    
    print("🚀 2025 Bitcoin Data Validation Tool")
    print("="*60)
    print(f"📋 Validating: {filename}")
    print(f"🎯 Target end time: {target_end_time}")
    print("="*60)
    
    is_valid = validate_2025_data(filename, target_end_time)
    
    if is_valid:
        print("\n🎉 2025 data validation passed!")
        sys.exit(0)
    else:
        print("\n💥 2025 data validation failed!")
        print("💡 Use find_missing_data.py and fetch_missing_data.py to fix gaps")
        sys.exit(1)

if __name__ == "__main__":
    main()
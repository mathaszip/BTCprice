import csv
from datetime import datetime, timezone, timedelta
import sys

def validate_bitcoin_data(filename):
    """
    Validate that Bitcoin 1-minute candle data has no missing timestamps
    """
    print(f"Validating data completeness in {filename}...")
    
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
        
        print(f"✓ Successfully read {row_count:,} rows from {filename}")
        
    except FileNotFoundError:
        print(f"❌ Error: File {filename} not found!")
        return False
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False
    
    if not timestamps:
        print("❌ No data found in file!")
        return False
    
    # Sort timestamps to ensure proper order
    timestamps.sort()
    
    # Get date range
    start_time = datetime.fromtimestamp(timestamps[0], timezone.utc)
    end_time = datetime.fromtimestamp(timestamps[-1], timezone.utc)
    
    print(f"📅 Data range: {start_time} to {end_time}")
    print(f"📊 Duration: {end_time - start_time}")
    
    # Calculate expected number of minutes
    total_minutes = int((timestamps[-1] - timestamps[0]) / 60) + 1
    print(f"⏱️  Expected minutes: {total_minutes:,}")
    print(f"📈 Actual candles: {len(timestamps):,}")
    
    # Check for missing timestamps (should be every 60 seconds)
    missing_timestamps = []
    duplicate_timestamps = []
    
    expected_timestamp = timestamps[0]
    prev_timestamp = None
    
    for i, timestamp in enumerate(timestamps):
        # Check for duplicates
        if timestamp == prev_timestamp:
            duplicate_timestamps.append(timestamp)
        
        # Check for missing timestamps
        while expected_timestamp < timestamp:
            missing_timestamps.append(expected_timestamp)
            expected_timestamp += 60
        
        if expected_timestamp == timestamp:
            expected_timestamp += 60
        
        prev_timestamp = timestamp
    
    # Report results
    print("\n" + "="*50)
    print("VALIDATION RESULTS")
    print("="*50)
    
    if not missing_timestamps and not duplicate_timestamps:
        print("✅ SUCCESS: Data is complete with no missing timestamps!")
        print(f"✅ All {len(timestamps):,} candles are present and accounted for")
        return True
    else:
        if missing_timestamps:
            print(f"❌ MISSING DATA: {len(missing_timestamps)} missing timestamps found!")
            print("\nFirst 10 missing timestamps:")
            for i, ts in enumerate(missing_timestamps[:10]):
                missing_time = datetime.fromtimestamp(ts, timezone.utc)
                print(f"  {i+1}. {missing_time} (timestamp: {ts})")
            
            if len(missing_timestamps) > 10:
                print(f"  ... and {len(missing_timestamps) - 10} more")
        
        if duplicate_timestamps:
            print(f"❌ DUPLICATE DATA: {len(duplicate_timestamps)} duplicate timestamps found!")
            print("\nFirst 10 duplicate timestamps:")
            for i, ts in enumerate(duplicate_timestamps[:10]):
                dup_time = datetime.fromtimestamp(ts, timezone.utc)
                print(f"  {i+1}. {dup_time} (timestamp: {ts})")
            
            if len(duplicate_timestamps) > 10:
                print(f"  ... and {len(duplicate_timestamps) - 10} more")
        
        return False

def main():
    # Check command line arguments
    if len(sys.argv) > 1:
        # If arguments provided, validate those specific files
        filenames = sys.argv[1:]
    else:
        # Default: validate all available years
        filenames = [
            'BTCUSD_1m_candles_2015.csv',
            'BTCUSD_1m_candles_2016.csv',
            'BTCUSD_1m_candles_2017.csv', 
            'BTCUSD_1m_candles_2018.csv'
        ]
    
    print("🚀 Bitcoin Data Validation Tool")
    print("="*70)
    print(f"📋 Validating {len(filenames)} file(s):")
    for filename in filenames:
        print(f"   • {filename}")
    print("="*70)
    
    all_valid = True
    results = {}
    
    for i, filename in enumerate(filenames):
        print(f"\n[{i+1}/{len(filenames)}] Checking {filename}...")
        print("-" * 50)
        
        is_valid = validate_bitcoin_data(filename)
        results[filename] = is_valid
        
        if not is_valid:
            all_valid = False
    
    # Summary report
    print("\n" + "="*70)
    print("📊 SUMMARY REPORT")
    print("="*70)
    
    for filename, is_valid in results.items():
        status = "✅ PASS" if is_valid else "❌ FAIL"
        print(f"{status} - {filename}")
    
    print("-" * 70)
    
    if all_valid:
        print(f"🎉 ALL FILES VALIDATED SUCCESSFULLY!")
        print(f"✅ {len(filenames)} out of {len(filenames)} files passed validation")
        print("🚀 Rate limiting retries worked perfectly across all datasets!")
        sys.exit(0)
    else:
        failed_count = len([v for v in results.values() if not v])
        passed_count = len(filenames) - failed_count
        print(f"💥 VALIDATION FAILED!")
        print(f"✅ {passed_count} out of {len(filenames)} files passed validation")
        print(f"❌ {failed_count} file(s) had issues - check rate limiting or data gaps")
        sys.exit(1)

if __name__ == "__main__":
    main()
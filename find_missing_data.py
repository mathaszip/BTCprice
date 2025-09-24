import csv
from datetime import datetime, timezone
import sys
import json

def find_missing_timestamps(filename):
    """
    Find all missing timestamps in Bitcoin 1-minute candle data
    Returns a list of missing timestamp ranges
    """
    print(f"Analyzing missing data in {filename}...")
    
    timestamps = []
    
    # Read all timestamps from the CSV
    try:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                unix_timestamp = int(row['unix_timestamp'])
                timestamps.append(unix_timestamp)
        
        print(f"✓ Read {len(timestamps):,} timestamps from {filename}")
        
    except FileNotFoundError:
        print(f"❌ Error: File {filename} not found!")
        return []
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return []
    
    if not timestamps:
        print("❌ No data found in file!")
        return []
    
    # Sort timestamps
    timestamps.sort()
    
    # Find missing timestamp ranges
    missing_ranges = []
    current_missing_start = None
    current_missing_end = None
    
    expected_timestamp = timestamps[0]
    
    for timestamp in timestamps:
        # Check if there's a gap before this timestamp
        if expected_timestamp < timestamp:
            # Found missing data
            if current_missing_start is None:
                current_missing_start = expected_timestamp
            current_missing_end = timestamp - 60  # Last missing timestamp
            
            # Add all missing timestamps in this gap
            missing_ts = expected_timestamp
            while missing_ts < timestamp:
                missing_ranges.append(missing_ts)
                missing_ts += 60
        
        # Update expected timestamp for next iteration
        expected_timestamp = timestamp + 60
    
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

def save_missing_data_info(filename, missing_timestamps, ranges):
    """
    Save missing data information to JSON files
    """
    base_name = filename.replace('.csv', '')
    
    # Save individual missing timestamps
    timestamps_file = f"{base_name}_missing_timestamps.json"
    with open(timestamps_file, 'w') as f:
        json.dump({
            'filename': filename,
            'total_missing': len(missing_timestamps),
            'missing_timestamps': missing_timestamps
        }, f, indent=2)
    
    # Save ranges for efficient fetching
    ranges_file = f"{base_name}_missing_ranges.json"
    range_info = []
    for start_ts, end_ts in ranges:
        start_dt = datetime.fromtimestamp(start_ts, timezone.utc)
        end_dt = datetime.fromtimestamp(end_ts, timezone.utc)
        duration_minutes = (end_ts - start_ts) // 60 + 1
        
        range_info.append({
            'start_timestamp': start_ts,
            'end_timestamp': end_ts,
            'start_datetime': start_dt.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'end_datetime': end_dt.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'duration_minutes': duration_minutes
        })
    
    with open(ranges_file, 'w') as f:
        json.dump({
            'filename': filename,
            'total_missing_timestamps': len(missing_timestamps),
            'total_ranges': len(ranges),
            'ranges': range_info
        }, f, indent=2)
    
    return timestamps_file, ranges_file

def main():
    if len(sys.argv) != 2:
        print("Usage: python find_missing_data.py <csv_filename>")
        print("Example: python find_missing_data.py BTCUSD_1m_candles_2018.csv")
        sys.exit(1)
    
    filename = sys.argv[1]
    
    print("🔍 Missing Data Analysis Tool")
    print("="*50)
    
    # Find missing timestamps
    missing_timestamps = find_missing_timestamps(filename)
    
    if not missing_timestamps:
        print("✅ No missing data found!")
        sys.exit(0)
    
    print(f"\n📊 ANALYSIS RESULTS")
    print("="*50)
    print(f"❌ Missing timestamps: {len(missing_timestamps):,}")
    
    # Group into ranges
    ranges = group_consecutive_timestamps(missing_timestamps)
    print(f"📦 Missing data ranges: {len(ranges)}")
    
    # Show details of ranges
    print(f"\n📋 MISSING DATA RANGES:")
    print("-"*50)
    total_missing_hours = len(missing_timestamps) / 60
    
    for i, (start_ts, end_ts) in enumerate(ranges, 1):
        start_dt = datetime.fromtimestamp(start_ts, timezone.utc)
        end_dt = datetime.fromtimestamp(end_ts, timezone.utc)
        duration_minutes = (end_ts - start_ts) // 60 + 1
        duration_hours = duration_minutes / 60
        
        print(f"  {i:2d}. {start_dt} to {end_dt}")
        print(f"      Duration: {duration_minutes:,} minutes ({duration_hours:.1f} hours)")
    
    print(f"\n📈 SUMMARY:")
    print(f"   Total missing time: {total_missing_hours:.1f} hours ({total_missing_hours/24:.1f} days)")
    
    # Save to files
    timestamps_file, ranges_file = save_missing_data_info(filename, missing_timestamps, ranges)
    
    print(f"\n💾 SAVED FILES:")
    print(f"   📄 Individual timestamps: {timestamps_file}")
    print(f"   📦 Ranges for fetching: {ranges_file}")
    
    print(f"\n✅ Analysis complete! Use the ranges file to fetch missing data.")

if __name__ == "__main__":
    main()
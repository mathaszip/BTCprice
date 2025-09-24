import csv
from datetime import datetime, timezone
import sys

def merge_csv_files(original_file, missing_data_file, output_file):
    """
    Merge original CSV with missing data CSV and sort by timestamp
    """
    print(f"📋 Merging {original_file} with {missing_data_file}...")
    
    all_data = []
    
    # Read original file
    try:
        with open(original_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                all_data.append(row)
        print(f"✅ Read {len(all_data):,} rows from {original_file}")
    except FileNotFoundError:
        print(f"❌ Error: {original_file} not found!")
        return False
    
    # Read missing data file (if it exists)
    missing_count = 0
    try:
        with open(missing_data_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                all_data.append(row)
                missing_count += 1
        print(f"✅ Read {missing_count:,} rows from {missing_data_file}")
    except FileNotFoundError:
        print(f"⚠️  Missing data file {missing_data_file} not found - using original data only")
    
    # Sort by unix timestamp
    all_data.sort(key=lambda x: int(x['unix_timestamp']))
    
    # Remove duplicates (keep first occurrence)
    seen_timestamps = set()
    unique_data = []
    duplicates_removed = 0
    
    for row in all_data:
        timestamp = int(row['unix_timestamp'])
        if timestamp not in seen_timestamps:
            seen_timestamps.add(timestamp)
            unique_data.append(row)
        else:
            duplicates_removed += 1
    
    if duplicates_removed > 0:
        print(f"🧹 Removed {duplicates_removed} duplicate timestamps")
    
    # Write merged file
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'open', 'close', 'volume', 'unix_timestamp', 'high', 'low']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in unique_data:
            writer.writerow(row)
    
    print(f"💾 Saved {len(unique_data):,} rows to {output_file}")
    return True

def main():
    if len(sys.argv) < 3:
        print("Usage: python merge_csv_data.py <original_file> <missing_data_file> [output_file]")
        print("Example: python merge_csv_data.py BTCUSD_1m_candles_2018.csv BTCUSD_1m_candles_2018_missing_data.csv BTCUSD_1m_candles_2018_complete.csv")
        sys.exit(1)
    
    original_file = sys.argv[1]
    missing_data_file = sys.argv[2]
    
    if len(sys.argv) >= 4:
        output_file = sys.argv[3]
    else:
        # Generate output filename
        base_name = original_file.replace('.csv', '')
        output_file = f"{base_name}_complete.csv"
    
    print("🔄 CSV Data Merger")
    print("="*50)
    
    success = merge_csv_files(original_file, missing_data_file, output_file)
    
    if success:
        print("\n✅ Merge completed successfully!")
        print(f"📄 Output file: {output_file}")
        print("\n📋 Next steps:")
        print(f"   1. Run: python validate_data.py {output_file}")
        print(f"   2. Check if all missing data has been filled")
    else:
        print("\n❌ Merge failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
import csv
import os

def dedupe_year(year):
    """Remove duplicate entries from a specific year's BTC data"""

    input_file = f'data/btc/BTCUSD_1m_candles_{year}.csv'
    output_file = f'data/btc/BTCUSD_1m_candles_{year}_deduped.csv'

    print(f"🔧 Removing duplicates from {year} data...")

    seen_timestamps = set()
    duplicates_removed = 0
    total_rows = 0

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Write header
        header = next(reader)
        writer.writerow(header)

        for row in reader:
            total_rows += 1
            unix_ts = row[4]  # unix_timestamp column

            if unix_ts not in seen_timestamps:
                seen_timestamps.add(unix_ts)
                writer.writerow(row)
            else:
                duplicates_removed += 1

    print(f"📊 Processed {total_rows} rows")
    print(f"🗑️  Removed {duplicates_removed} duplicates")
    print(f"💾 Saved {total_rows - duplicates_removed} unique rows to {output_file}")

    if duplicates_removed > 0:
        # Replace original file
        backup_file = input_file + '.backup'
        os.rename(input_file, backup_file)
        os.rename(output_file, input_file)
        print(f"✅ Replaced original file (backup saved as {backup_file})")
    else:
        # No duplicates, just remove the temp file
        os.remove(output_file)
        print("✅ No duplicates found")

def dedupe_all_years():
    """Check and dedupe all years if needed"""
    for year in range(2011, 2026):
        print(f"\n🔍 Checking {year}...")
        dedupe_year(year)

if __name__ == "__main__":
    dedupe_all_years()
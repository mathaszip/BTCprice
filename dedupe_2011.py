import csv
import os

def remove_duplicates_from_2011():
    """Remove duplicate entries from 2011 BTC data"""

    input_file = 'data/btc/BTCUSD_1m_candles_2011.csv'
    output_file = 'data/btc/BTCUSD_1m_candles_2011_deduped.csv'

    print("🔧 Removing duplicates from 2011 data...")

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
                if duplicates_removed <= 5:  # Show first few duplicates
                    print(f"🗑️  Removed duplicate: {row[0]} (unix: {unix_ts})")

    print(f"📊 Processed {total_rows} rows")
    print(f"🗑️  Removed {duplicates_removed} duplicates")
    print(f"💾 Saved {total_rows - duplicates_removed} unique rows to {output_file}")

    # Replace original file
    backup_file = input_file + '.backup'
    os.rename(input_file, backup_file)
    os.rename(output_file, input_file)

    print(f"✅ Replaced original file (backup saved as {backup_file})")

if __name__ == "__main__":
    remove_duplicates_from_2011()
import csv

def fix_corrupted_2015_data():
    """Fix the corrupted data point in 2015"""

    input_file = 'data/btc/BTCUSD_1m_candles_2015.csv'
    output_file = 'data/btc/BTCUSD_1m_candles_2015_fixed.csv'

    print("🔧 Fixing corrupted data in 2015...")

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Write header
        header = next(reader)
        writer.writerow(header)

        prev_row = None
        fixed_count = 0

        for row_num, row in enumerate(reader, 1):
            # Check if this is the corrupted row (all prices are 0)
            try:
                open_price = float(row[1])
                close_price = float(row[2])
                high_price = float(row[5])
                low_price = float(row[6])

                if open_price == 0 and close_price == 0 and high_price == 0 and low_price == 0:
                    if prev_row:
                        # Replace with previous row's data but update timestamp
                        fixed_row = row.copy()
                        fixed_row[1] = prev_row[1]  # open
                        fixed_row[2] = prev_row[2]  # close
                        fixed_row[5] = prev_row[5]  # high
                        fixed_row[6] = prev_row[6]  # low
                        fixed_row[3] = '0.0'       # volume = 0 for missing data

                        writer.writerow(fixed_row)
                        fixed_count += 1
                        print(f"🔧 Fixed row {row_num}: {row[0]} - replaced zeros with {prev_row[1]}")
                    else:
                        # No previous data, skip this row
                        print(f"⚠️  Skipping row {row_num}: {row[0]} - no previous data to use")
                        continue
                else:
                    writer.writerow(row)
                    prev_row = row

            except (ValueError, IndexError):
                print(f"⚠️  Skipping malformed row {row_num}")
                continue

    print(f"📊 Fixed {fixed_count} corrupted rows")
    print(f"💾 Saved to {output_file}")

    # Replace original file
    import os
    backup_file = input_file + '.backup'
    os.rename(input_file, backup_file)
    os.rename(output_file, input_file)
    print(f"✅ Replaced original file (backup saved as {backup_file})")

if __name__ == "__main__":
    fix_corrupted_2015_data()
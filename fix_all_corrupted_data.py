import csv
import os

def fix_corrupted_data_all_years():
    """Fix corrupted data points (all zeros) in all years"""

    data_dir = 'data/btc'
    total_fixed = 0

    for year in range(2011, 2026):
        filename = f'BTCUSD_1m_candles_{year}.csv'
        filepath = os.path.join(data_dir, filename)

        if not os.path.exists(filepath):
            continue

        print(f"🔍 Checking {year}...")

        # Read and fix data
        rows = []
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows.append(header)

            prev_valid_row = None
            fixed_this_year = 0

            for row in reader:
                try:
                    open_price = float(row[1])
                    close_price = float(row[2])
                    high_price = float(row[5])
                    low_price = float(row[6])

                    # Check if all prices are zero (corrupted)
                    if open_price == 0 and close_price == 0 and high_price == 0 and low_price == 0:
                        if prev_valid_row:
                            # Fix by copying previous valid prices
                            fixed_row = row.copy()
                            fixed_row[1] = prev_valid_row[1]  # open
                            fixed_row[2] = prev_valid_row[2]  # close
                            fixed_row[5] = prev_valid_row[5]  # high
                            fixed_row[6] = prev_valid_row[6]  # low
                            fixed_row[3] = '0.0'             # volume = 0

                            rows.append(fixed_row)
                            fixed_this_year += 1
                            total_fixed += 1
                        else:
                            # No previous data, skip this row
                            continue
                    else:
                        rows.append(row)
                        prev_valid_row = row

                except (ValueError, IndexError):
                    continue

        # Write back if any fixes were made
        if fixed_this_year > 0:
            backup_file = filepath + '.backup3'
            os.rename(filepath, backup_file)

            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(rows)

            print(f"🔧 {year}: Fixed {fixed_this_year} corrupted rows")
        else:
            print(f"✅ {year}: No corrupted data found")

    print(f"\n📊 Total corrupted data points fixed across all years: {total_fixed}")

if __name__ == "__main__":
    fix_corrupted_data_all_years()
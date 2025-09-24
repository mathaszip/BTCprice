import csv
import os
from datetime import datetime, timedelta

def fill_year_boundary_gaps():
    """Fill missing data points at year boundaries by copying previous prices"""

    data_dir = 'data/btc'

    # Gaps to fill: (year_file_to_modify, missing_timestamp, previous_year_file_to_get_price_from)
    gaps_to_fill = [
        ('2012', '2012-01-01 00:00:00', '2011'),  # Missing 2012-01-01 00:00:00, get price from 2011 end
        ('2013', '2013-01-01 00:00:00', '2012'),  # Missing 2013-01-01 00:00:00, get price from 2012 end
        ('2014', '2014-01-01 00:00:00', '2013'),  # Missing 2014-01-01 00:00:00, get price from 2013 end
        ('2015', '2015-01-01 00:00:00', '2014'),  # Missing 2015-01-01 00:00:00, get price from 2014 end
    ]

    total_inserted = 0

    for target_year, missing_ts_str, source_year in gaps_to_fill:
        target_file = f'BTCUSD_1m_candles_{target_year}.csv'
        source_file = f'BTCUSD_1m_candles_{source_year}.csv'
        target_path = os.path.join(data_dir, target_file)
        source_path = os.path.join(data_dir, source_file)

        print(f"🔧 Filling gap: {missing_ts_str} in {target_year} (using {source_year} prices)")

        # Get the last row from source year
        source_last_row = None
        with open(source_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for row in reader:
                source_last_row = row

        if not source_last_row:
            print(f"❌ Could not read source data from {source_year}")
            continue

        # Parse missing timestamp
        missing_ts = datetime.strptime(missing_ts_str, '%Y-%m-%d %H:%M:%S')
        missing_unix = int(missing_ts.timestamp())

        # Create the missing row
        missing_row = [
            missing_ts.strftime('%Y-%m-%d %H:%M:%S'),  # timestamp
            source_last_row[1],  # open (same as source year end)
            source_last_row[2],  # close (same as source year end)
            '0.0',               # volume = 0
            str(missing_unix),   # unix timestamp
            source_last_row[5],  # high (same as source year end)
            source_last_row[6],  # low (same as source year end)
        ]

        # Read target file and insert the missing row at the beginning
        rows = []
        with open(target_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows.append(header)
            rows.append(missing_row)  # Insert at the beginning

            for row in reader:
                rows.append(row)

        # Write back
        backup_file = target_path + '.backup5'
        os.rename(target_path, backup_file)

        with open(target_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        total_inserted += 1
        print(f"✅ Inserted {missing_ts_str} into {target_file}")

    print(f"\n📊 Total rows inserted: {total_inserted}")

if __name__ == "__main__":
    fill_year_boundary_gaps()
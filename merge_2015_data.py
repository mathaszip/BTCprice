import csv
import os
from datetime import datetime

def merge_2015_data():
    # File paths
    bitstamp_file = 'BITSTAMP_BTCUSD_1m_candles_2015.csv'
    coinbase_file = 'COINBASE_BTCUSD_1m_candles_2015.csv'
    output_file = 'BTCUSD_1m_candles_2015_merged.csv'

    print("🔍 Analyzing data overlap...")

    # Read Bitstamp data
    bitstamp_data = []
    with open(bitstamp_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        for row in reader:
            bitstamp_data.append(row)

    # Read Coinbase data
    coinbase_data = []
    with open(coinbase_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        for row in reader:
            coinbase_data.append(row)

    print(f"📊 Bitstamp: {len(bitstamp_data)} candles")
    print(f"📊 Coinbase: {len(coinbase_data)} candles")

    # Find overlap timestamp
    bitstamp_last = bitstamp_data[-1]
    coinbase_first = coinbase_data[0]

    print(f"🔗 Bitstamp ends: {bitstamp_last[0]}")
    print(f"🔗 Coinbase starts: {coinbase_first[0]}")

    if bitstamp_last[0] == coinbase_first[0]:
        print("⚠️  OVERLAP DETECTED: Same timestamp with different data!")
        print(f"   Bitstamp: O={bitstamp_last[1]} H={bitstamp_last[5]} L={bitstamp_last[6]} C={bitstamp_last[2]} V={bitstamp_last[3]}")
        print(f"   Coinbase: O={coinbase_first[1]} H={coinbase_first[5]} L={coinbase_first[6]} C={coinbase_first[2]} V={coinbase_first[3]}")

        # Ask user how to handle overlap
        print("\n❓ How to handle the overlapping timestamp?")
        print("1. Keep Bitstamp data (prefer historical source)")
        print("2. Keep Coinbase data (prefer recent source)")
        print("3. Keep both (will duplicate timestamp)")
        choice = input("Enter choice (1/2/3): ").strip()

        if choice == '1':
            # Remove the overlapping timestamp from Coinbase
            coinbase_data = coinbase_data[1:]
            print("✅ Using Bitstamp data for overlap")
        elif choice == '2':
            # Remove the overlapping timestamp from Bitstamp
            bitstamp_data = bitstamp_data[:-1]
            print("✅ Using Coinbase data for overlap")
        else:
            print("✅ Keeping both entries (duplicate timestamp)")

    # Merge the data
    merged_data = bitstamp_data + coinbase_data

    print(f"📈 Total merged candles: {len(merged_data)}")

    # Write merged data
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)  # Write header
        writer.writerows(merged_data)

    print(f"💾 Saved merged data to: {output_file}")
    print(f"📊 Final file size: {len(merged_data)} candles")

if __name__ == "__main__":
    merge_2015_data()
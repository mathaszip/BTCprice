import os
import glob
import csv

def combine_btc_csvs():
    btc_dir = 'data/btc'
    output_file = 'combined_btc_data.csv'
    btc_files = sorted(glob.glob(os.path.join(btc_dir, 'BTCUSD_1m_candles_*.csv')))
    
    if not btc_files:
        print("No BTC CSV files found")
        return
    
    print("Found", len(btc_files), "files")
    
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        header_written = False
        
        for csv_file in btc_files:
            print("Processing", os.path.basename(csv_file))
            
            with open(csv_file, 'r', newline='') as infile:
                reader = csv.reader(infile)
                rows = list(reader)
                
                if not rows:
                    continue
                
                first_row = rows[0]
                if first_row[0] == 'timestamp':
                    if not header_written:
                        writer.writerow(first_row)
                        header_written = True
                    data_rows = rows[1:]
                else:
                    data_rows = rows
                
                writer.writerows(data_rows)
                print("Added", len(data_rows), "rows")
    
    print("Done")

if __name__ == "__main__":
    combine_btc_csvs()
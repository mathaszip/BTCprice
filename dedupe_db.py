import psycopg2
import os

# Database configuration
DB_HOST = 'poop'
DB_PORT = '5432'
DB_NAME = 'binance_data'
DB_USER = 'postgres'
DB_PASSWORD = 'kurwa'

def remove_duplicates():
    """Remove duplicate records based on timestamp, keeping the one with the smallest id"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Find duplicates
        cursor.execute("""
            SELECT timestamp, COUNT(*) as count
            FROM btc_price_data
            GROUP BY timestamp
            HAVING COUNT(*) > 1
        """)

        duplicates = cursor.fetchall()
        print(f"Found {len(duplicates)} timestamps with duplicates")

        total_removed = 0
        for timestamp, count in duplicates:
            # Keep the record with the smallest id, delete others
            cursor.execute("""
                DELETE FROM btc_price_data
                WHERE timestamp = %s AND id NOT IN (
                    SELECT MIN(id)
                    FROM btc_price_data
                    WHERE timestamp = %s
                )
            """, (timestamp, timestamp))
            removed = cursor.rowcount
            total_removed += removed
            print(f"Removed {removed} duplicates for timestamp {timestamp}")

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Total duplicates removed: {total_removed}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    remove_duplicates()
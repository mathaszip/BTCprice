from flask import Flask, request, jsonify
import psycopg2
from datetime import datetime

app = Flask(__name__)

# Database configuration
DB_HOST = 'poop'
DB_PORT = '5432'
DB_NAME = 'binance_data'
DB_USER = 'postgres'
DB_PASSWORD = 'kurwa'

@app.route('/price', methods=['GET'])
def get_price():
    unix_ts = request.args.get('timestamp')
    if not unix_ts:
        return jsonify({"error": "Missing timestamp parameter"}), 400

    try:
        unix_ts = int(unix_ts)
        # Convert Unix timestamp to datetime
        dt = datetime.utcfromtimestamp(unix_ts)
    except ValueError:
        return jsonify({"error": "Invalid timestamp"}), 400

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Query the row
        cursor.execute("""
            SELECT id, timestamp, open_price, high_price, low_price, close_price, volume, created_at
            FROM btc_price_data
            WHERE timestamp = %s
        """, (dt,))

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            data = {
                "id": row[0],
                "timestamp": row[1].isoformat(),
                "open_price": str(row[2]),
                "high_price": str(row[3]),
                "low_price": str(row[4]),
                "close_price": str(row[5]),
                "volume": str(row[6]),
                "created_at": row[7].isoformat() if row[7] else None
            }
            return jsonify(data)
        else:
            return jsonify({"error": "No data found for this timestamp"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
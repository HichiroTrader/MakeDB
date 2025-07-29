#!/usr/bin/env python3
"""
Script đơn giản để thêm nhiều symbols cùng lúc
"""
import sys
import json
import redis
import psycopg2

def add_symbols(symbols_str):
    """Thêm nhiều symbols"""
    # Parse symbols
    symbols = [s.strip().upper() for s in symbols_str.split(',')]
    
    # Connect to database
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='rithmic_db',
        user='postgres',
        password='postgres'
    )
    
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379)
    
    cursor = conn.cursor()
    added = []
    
    for symbol in symbols:
        try:
            # Add to database
            cursor.execute("""
                INSERT INTO symbols (symbol, exchange, active)
                VALUES (%s, 'CME', TRUE)
                ON CONFLICT (symbol) DO UPDATE SET active = TRUE
            """, (symbol,))
            
            # Notify collector
            r.rpush('symbol_subscriptions', json.dumps({
                'symbol': symbol,
                'exchange': 'CME'
            }))
            
            added.append(symbol)
            print(f"✅ Added {symbol}")
            
        except Exception as e:
            print(f"❌ Error adding {symbol}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\n✅ Successfully added {len(added)} symbols: {', '.join(added)}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        add_symbols(sys.argv[1])
    else:
        symbols = input("Enter symbols (comma-separated): ")
        add_symbols(symbols)
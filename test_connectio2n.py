import socket
import os
from dotenv import load_dotenv

load_dotenv()

def test_connections():
    print("=== Testing Connections ===\n")
    
    # Test R|Trader Pro
    rtrader_port = int(os.getenv('RTRADER_PORT', 3013))
    print(f"1. Testing R|Trader Pro on port {rtrader_port}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', rtrader_port))
    sock.close()
    
    if result == 0:
        print(f"   ✅ R|Trader Pro is running on port {rtrader_port}")
    else:
        print(f"   ❌ R|Trader Pro is NOT running on port {rtrader_port}")
        print("   Please check:")
        print("   - Is R|Trader Pro running?")
        print("   - Is Plugin Mode enabled?")
        print("   - Is the port correct?")
    
    # Test Database
    print("\n2. Testing PostgreSQL...")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='rithmic_db',
            user='postgres',
            password='postgres'
        )
        conn.close()
        print("   ✅ PostgreSQL is running")
    except Exception as e:
        print(f"   ❌ PostgreSQL error: {e}")
    
    # Test Redis
    print("\n3. Testing Redis...")
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379)
        r.ping()
        print("   ✅ Redis is running")
    except Exception as e:
        print(f"   ❌ Redis error: {e}")

if __name__ == "__main__":
    test_connections()
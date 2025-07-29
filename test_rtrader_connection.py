#!/usr/bin/env python3
"""
Test R|Trader Pro Plugin Connection
"""
import socket
import json
import time

def test_connection():
    """Test connection to R|Trader Pro Plugin API"""
    host = 'localhost'  # Use localhost when running outside Docker
    port = 3013
        
    print(f"Testing connection to R|Trader Pro at {host}:{port}...")
    
    try:
        # Create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        
        # Connect
        sock.connect((host, port))
        print("✅ Connected successfully!")
        
        # Send test message
        test_msg = {
            'action': 'PING',
            'type': 'TEST',
            'timestamp': time.time()
        }
        
        msg_str = json.dumps(test_msg) + '\n'
        sock.send(msg_str.encode('utf-8'))
        print(f"📤 Sent: {test_msg}")
        
        # Try to receive response
        sock.settimeout(2)
        try:
            response = sock.recv(1024)
            if response:
                print(f"📥 Received: {response.decode('utf-8').strip()}")
            else:
                print("⚠️  No response received (this might be normal)")
        except socket.timeout:
            print("⏱️  Response timeout (this might be normal)")
        
        # Close connection
        sock.close()
        print("\n✅ Connection test completed!")
        
        return True
        
    except ConnectionRefusedError:
        print("\n❌ Connection refused!")
        print("\nPossible causes:")
        print("1. R|Trader Pro is not running")
        print("2. Plugin Mode is not enabled")
        print("3. Wrong port number (check R|Trader Pro settings)")
        return False
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nTroubleshooting steps:")
        print("1. Open R|Trader Pro")
        print("2. Go to Settings → Plugin Configuration")
        print("3. Enable Plugin Mode")
        print("4. Set port to 65000")
        print("5. Restart R|Trader Pro")
        return False

def test_market_data_subscription():
    """Test subscribing to market data"""
    host = 'localhost'
    port = 3010
        
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        
        # Subscribe to a symbol
        subscribe_msg = {
            'action': 'SUBSCRIBE',
            'type': 'MARKET_DATA',
            'symbol': 'GCQ5',
            'exchange': 'CME',
            'data_types': ['TRADE', 'QUOTE', 'DEPTH']
        }
        
        msg_str = json.dumps(subscribe_msg) + '\n'
        sock.send(msg_str.encode('utf-8'))
        print(f"\n📤 Sent subscription request for GCQ5")
        
        # Listen for data (10 seconds)
        print("👂 Listening for market data (10 seconds)...")
        sock.settimeout(1)
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < 10:
            try:
                data = sock.recv(4096)
                if data:
                    messages = data.decode('utf-8').strip().split('\n')
                    for msg in messages:
                        if msg:
                            message_count += 1
                            print(f"📥 Message {message_count}: {msg[:100]}...")
            except socket.timeout:
                continue
        
        sock.close()
        
        if message_count > 0:
            print(f"\n✅ Received {message_count} messages!")
        else:
            print("\n⚠️  No market data received")
            print("This could mean:")
            print("- Market is closed")
            print("- Symbol is incorrect")
            print("- No data feed in R|Trader Pro")
        
    except Exception as e:
        print(f"\n❌ Subscription test failed: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("R|Trader Pro Plugin Connection Test")
    print("=" * 60)
    
    # Test basic connection
    if test_connection():
        print("\n" + "=" * 60)
        print("Testing Market Data Subscription...")
        print("=" * 60)
        test_market_data_subscription()
    
    print("\n" + "=" * 60)
    print("Test completed!")
    print("=" * 60)

# test_host_connection.py
import socket

def test_port(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    result = sock.connect_ex((host, port))
    sock.close()
    return result == 0

# Test các port
for port in [3010, 3011, 3012, 3013]:
    if test_port('localhost', port):
        print(f"✅ Port {port} is OPEN")
    else:
        print(f"❌ Port {port} is CLOSED")
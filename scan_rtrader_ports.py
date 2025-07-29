import socket
import time

def scan_port(port):
    """Kiểm tra xem một port có đang mở trên localhost không."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)  # Đặt timeout ngắn để không phải chờ lâu
    result = sock.connect_ex(('localhost', port))
    sock.close()
    return result == 0

print("=== Scanning for R|Trader Pro ports ===\n")

# Danh sách các cổng phổ biến mà R|Trader Pro có thể sử dụng
ports_to_check = [
    3010, 3011, 3012, 3013,  # Cổng mặc định
    8000, 8001, 8080, 8081,  # Cổng thay thế
    5050, 5051,
    9090, 9091,
]

open_ports = []

# Quét tất cả các cổng trong danh sách
for port in ports_to_check:
    if scan_port(port):
        print(f"✅ Port {port} is OPEN")
        open_ports.append(port)
    else:
        print(f"❌ Port {port} is closed")
    time.sleep(0.1)  # Chờ một chút giữa mỗi lần quét

print(f"\n>>> Open ports found: {open_ports}\n")

# Cố gắng kết nối và gửi dữ liệu test tới các cổng đang mở
if open_ports:
    print("=== Testing connection to each open port ===")
    for port in open_ports:
        print(f"\n--- Testing port {port} ---")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)  # Đặt timeout kết nối là 2 giây
            sock.connect(('localhost', port))
            print(f"  [+] Connection successful.")

            # Cố gắng gửi một tin nhắn test đơn giản
            test_msg = b"PING\n"
            sock.send(test_msg)
            print(f"  [>] Sent a test message: {test_msg.strip()}")

            # Cố gắng nhận phản hồi
            sock.settimeout(1)  # Chờ phản hồi trong 1 giây
            try:
                data = sock.recv(1024)
                print(f"  [<] Received response: {data[:100]}") # In 100 bytes đầu tiên
            except socket.timeout:
                print(f"  [!] No immediate response (timeout). This is normal for some protocols.")

            sock.close()
            print(f"  [-] Connection closed.")
        except Exception as e:
            # Đây là dòng đã được sửa
            print(f"  [!] Error during test: {e}")

print("\n=== Scan Complete ===")
#!/usr/bin/env python3
"""
Database Manager cho R|Trader Pro Data Collection
Công cụ dễ sử dụng cho trader - không cần biết code!
"""

import os
import sys
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
import json

class DatabaseManager:
    def __init__(self):
        # Kết nối database
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'rithmic_db',
            'user': 'postgres',
            'password': 'postgres'  # Password từ docker-compose.yml
        }
        self.conn = None
        
    def connect(self):
        """Kết nối database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            print("✅ Đã kết nối database thành công!")
            return True
        except Exception as e:
            print(f"❌ Lỗi kết nối database: {e}")
            print("\n📝 Hướng dẫn:")
            print("1. Đảm bảo Docker Desktop đang chạy")
            print("2. Chạy: docker-compose up -d")
            print("3. Đợi 30 giây rồi thử lại")
            return False
    
    def show_menu(self):
        """Hiển thị menu chính"""
        print("\n" + "="*60)
        print("🔧 QUẢN LÝ DATABASE R|TRADER PRO")
        print("="*60)
        print("1. 📊 Xem thống kê dữ liệu")
        print("2. 📈 Xem dữ liệu tick gần nhất")
        print("3. 📉 Xem dữ liệu Level 2")
        print("4. ➕ Thêm symbol mới")
        print("5. 📋 Xem danh sách symbols")
        print("6. 💾 Export dữ liệu ra CSV")
        print("7. 🗑️  Xóa dữ liệu cũ")
        print("8. 📦 Backup database")
        print("9. 📂 Restore database")
        print("10. ℹ️  Thông tin kết nối")
        print("0. ❌ Thoát")
        print("="*60)
        
    def show_statistics(self):
        """Xem thống kê dữ liệu"""
        cursor = self.conn.cursor()
        
        print("\n📊 THỐNG KÊ DỮ LIỆU")
        print("-"*60)
        
        # Tổng số tick data
        cursor.execute("SELECT COUNT(*) FROM tick_data")
        total_ticks = cursor.fetchone()[0]
        print(f"📈 Tổng số tick data: {total_ticks:,}")
        
        # Tổng số level2 data
        cursor.execute("SELECT COUNT(*) FROM level2_data")
        total_level2 = cursor.fetchone()[0]
        print(f"📉 Tổng số Level 2 data: {total_level2:,}")
        
        # Thống kê theo symbol
        print("\n📊 Thống kê theo Symbol:")
        cursor.execute("""
            SELECT symbol, 
                   COUNT(*) as tick_count,
                   MIN(timestamp) as first_tick,
                   MAX(timestamp) as last_tick
            FROM tick_data
            GROUP BY symbol
            ORDER BY tick_count DESC
        """)
        
        data = cursor.fetchall()
        if data:
            headers = ["Symbol", "Số lượng", "Tick đầu tiên", "Tick cuối cùng"]
            print(tabulate(data, headers=headers, tablefmt="grid"))
        else:
            print("⚠️  Chưa có dữ liệu!")
            
        cursor.close()
    
    def show_recent_ticks(self):
        """Xem tick data gần nhất"""
        symbol = input("\n📊 Nhập symbol (hoặc Enter để xem tất cả): ").upper()
        limit = input("📊 Số lượng tick muốn xem (mặc định 20): ")
        limit = int(limit) if limit else 20
        
        cursor = self.conn.cursor()
        
        if symbol:
            query = """
                SELECT timestamp, symbol, price, size, bid_price, ask_price
                FROM tick_data
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """
            cursor.execute(query, (symbol, limit))
        else:
            query = """
                SELECT timestamp, symbol, price, size, bid_price, ask_price
                FROM tick_data
                ORDER BY timestamp DESC
                LIMIT %s
            """
            cursor.execute(query, (limit,))
        
        data = cursor.fetchall()
        if data:
            headers = ["Thời gian", "Symbol", "Giá", "Size", "Bid", "Ask"]
            print(f"\n📈 {limit} tick gần nhất:")
            print(tabulate(data, headers=headers, tablefmt="grid"))
        else:
            print("⚠️  Không có dữ liệu!")
            
        cursor.close()
    
    def show_level2_data(self):
        """Xem Level 2 data"""
        symbol = input("\n📊 Nhập symbol: ").upper()
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT timestamp, side, level, price, size
            FROM level2_data
            WHERE symbol = %s AND timestamp = (
                SELECT MAX(timestamp) FROM level2_data WHERE symbol = %s
            )
            ORDER BY side DESC, level
        """, (symbol, symbol))
        
        data = cursor.fetchall()
        if data:
            print(f"\n📉 Level 2 data cho {symbol}:")
            bids = [(d[2], d[3], d[4]) for d in data if d[1] == 'B']
            asks = [(d[2], d[3], d[4]) for d in data if d[1] == 'S']
            
            print("\n🟢 BIDS:")
            print(tabulate(bids, headers=["Level", "Price", "Size"], tablefmt="grid"))
            
            print("\n🔴 ASKS:")
            print(tabulate(asks, headers=["Level", "Price", "Size"], tablefmt="grid"))
        else:
            print(f"⚠️  Không có Level 2 data cho {symbol}!")
            
        cursor.close()
    
    def add_symbol(self):
        """Thêm symbol mới"""
        symbol = input("\n📊 Nhập symbol mới (VD: ESH5): ").upper()
        exchange = input("📊 Nhập exchange (mặc định CME): ") or "CME"
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO symbols (symbol, exchange, active)
                VALUES (%s, %s, TRUE)
                ON CONFLICT (symbol) DO UPDATE SET active = TRUE
            """, (symbol, exchange))
            self.conn.commit()
            print(f"✅ Đã thêm symbol {symbol} trên {exchange}!")
            
            # Thông báo cho collector
            import redis
            r = redis.Redis(host='localhost', port=6379)
            r.rpush('symbol_subscriptions', json.dumps({
                'symbol': symbol,
                'exchange': exchange
            }))
            print("📤 Đã gửi yêu cầu subscribe đến collector!")
            
        except Exception as e:
            print(f"❌ Lỗi: {e}")
            self.conn.rollback()
        
        cursor.close()
    
    def list_symbols(self):
        """Xem danh sách symbols"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.symbol, s.exchange, s.active, 
                   COUNT(t.id) as tick_count,
                   MAX(t.timestamp) as last_update
            FROM symbols s
            LEFT JOIN tick_data t ON s.symbol = t.symbol
            GROUP BY s.symbol, s.exchange, s.active
            ORDER BY s.symbol
        """)
        
        data = cursor.fetchall()
        if data:
            print("\n📋 Danh sách Symbols:")
            headers = ["Symbol", "Exchange", "Active", "Ticks", "Cập nhật cuối"]
            formatted_data = []
            for row in data:
                formatted_data.append([
                    row[0], row[1], 
                    "✅" if row[2] else "❌",
                    f"{row[3]:,}" if row[3] else "0",
                    row[4].strftime("%Y-%m-%d %H:%M:%S") if row[4] else "N/A"
                ])
            print(tabulate(formatted_data, headers=headers, tablefmt="grid"))
        else:
            print("⚠️  Chưa có symbol nào!")
            
        cursor.close()
    
    def export_to_csv(self):
        """Export dữ liệu ra CSV"""
        symbol = input("\n📊 Nhập symbol cần export: ").upper()
        days = input("📊 Số ngày dữ liệu (mặc định 1): ")
        days = int(days) if days else 1
        
        # Tạo thư mục export nếu chưa có
        os.makedirs("exports", exist_ok=True)
        
        # Export tick data
        query = """
            SELECT * FROM tick_data
            WHERE symbol = %s 
            AND timestamp > NOW() - INTERVAL '%s days'
            ORDER BY timestamp
        """
        
        df = pd.read_sql(query, self.conn, params=(symbol, days))
        if not df.empty:
            filename = f"exports/{symbol}_ticks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Đã export {len(df)} tick records đến {filename}")
        else:
            print("⚠️  Không có dữ liệu để export!")
    
    def cleanup_old_data(self):
        """Xóa dữ liệu cũ"""
        days = input("\n📊 Xóa dữ liệu cũ hơn bao nhiêu ngày? (mặc định 30): ")
        days = int(days) if days else 30
        
        confirm = input(f"⚠️  Bạn chắc chắn muốn xóa dữ liệu cũ hơn {days} ngày? (y/N): ")
        if confirm.lower() != 'y':
            print("❌ Đã hủy!")
            return
        
        cursor = self.conn.cursor()
        
        # Xóa tick data cũ
        cursor.execute("""
            DELETE FROM tick_data 
            WHERE timestamp < NOW() - INTERVAL '%s days'
        """, (days,))
        tick_deleted = cursor.rowcount
        
        # Xóa level2 data cũ
        cursor.execute("""
            DELETE FROM level2_data 
            WHERE timestamp < NOW() - INTERVAL '%s days'
        """, (days,))
        level2_deleted = cursor.rowcount
        
        self.conn.commit()
        cursor.close()
        
        print(f"✅ Đã xóa {tick_deleted:,} tick records và {level2_deleted:,} level2 records!")
    
    def backup_database(self):
        """Backup database"""
        print("\n📦 Backup Database")
        backup_name = input("📦 Tên file backup (mặc định: rithmic_backup_[timestamp]): ")
        
        if not backup_name:
            backup_name = f"rithmic_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Tạo thư mục backup nếu chưa có
        os.makedirs("backups", exist_ok=True)
        
        backup_file = f"backups/{backup_name}.sql"
        
        # Chạy pg_dump
        cmd = f'docker-compose exec -T db pg_dump -U postgres rithmic_db > {backup_file}'
        
        print(f"⏳ Đang backup database...")
        result = os.system(cmd)
        
        if result == 0:
            print(f"✅ Backup thành công: {backup_file}")
            # Kiểm tra kích thước file
            size = os.path.getsize(backup_file) / (1024 * 1024)  # MB
            print(f"📊 Kích thước: {size:.2f} MB")
        else:
            print("❌ Backup thất bại!")
    
    def restore_database(self):
        """Restore database từ backup"""
        print("\n📂 Restore Database")
        
        # Liệt kê các file backup
        if os.path.exists("backups"):
            backups = [f for f in os.listdir("backups") if f.endswith('.sql')]
            if backups:
                print("\n📋 Các file backup có sẵn:")
                for i, backup in enumerate(backups, 1):
                    size = os.path.getsize(f"backups/{backup}") / (1024 * 1024)
                    print(f"{i}. {backup} ({size:.2f} MB)")
                
                choice = input("\n📂 Chọn số thứ tự file backup: ")
                try:
                    backup_file = f"backups/{backups[int(choice) - 1]}"
                except:
                    print("❌ Lựa chọn không hợp lệ!")
                    return
            else:
                print("⚠️  Không có file backup nào!")
                return
        else:
            print("⚠️  Thư mục backup không tồn tại!")
            return
        
        confirm = input(f"\n⚠️  Restore sẽ XÓA TẤT CẢ dữ liệu hiện tại! Tiếp tục? (y/N): ")
        if confirm.lower() != 'y':
            print("❌ Đã hủy!")
            return
        
        print(f"⏳ Đang restore từ {backup_file}...")
        cmd = f'docker-compose exec -T db psql -U postgres rithmic_db < {backup_file}'
        result = os.system(cmd)
        
        if result == 0:
            print("✅ Restore thành công!")
        else:
            print("❌ Restore thất bại!")
    
    def show_connection_info(self):
        """Hiển thị thông tin kết nối"""
        print("\n🔌 THÔNG TIN KẾT NỐI DATABASE")
        print("-"*60)
        print(f"🏠 Host: {self.db_config['host']}")
        print(f"🔌 Port: {self.db_config['port']}")
        print(f"📊 Database: {self.db_config['database']}")
        print(f"👤 User: {self.db_config['user']}")
        print(f"🔑 Password: {'*' * len(self.db_config['password'])}")
        print("\n📝 Câu lệnh kết nối:")
        print(f"psql -h {self.db_config['host']} -p {self.db_config['port']} -U {self.db_config['user']} -d {self.db_config['database']}")
        print("\n🐳 Kết nối từ Docker:")
        print("docker-compose exec db psql -U postgres -d rithmic_db")
    
    def run(self):
        """Chạy chương trình chính"""
        if not self.connect():
            return
        
        while True:
            self.show_menu()
            choice = input("\n👉 Chọn chức năng: ")
            
            try:
                if choice == '0':
                    print("\n👋 Tạm biệt!")
                    break
                elif choice == '1':
                    self.show_statistics()
                elif choice == '2':
                    self.show_recent_ticks()
                elif choice == '3':
                    self.show_level2_data()
                elif choice == '4':
                    self.add_symbol()
                elif choice == '5':
                    self.list_symbols()
                elif choice == '6':
                    self.export_to_csv()
                elif choice == '7':
                    self.cleanup_old_data()
                elif choice == '8':
                    self.backup_database()
                elif choice == '9':
                    self.restore_database()
                elif choice == '10':
                    self.show_connection_info()
                else:
                    print("❌ Lựa chọn không hợp lệ!")
                
                input("\n📌 Nhấn Enter để tiếp tục...")
                
            except Exception as e:
                print(f"\n❌ Lỗi: {e}")
                input("\n📌 Nhấn Enter để tiếp tục...")
        
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    print("🚀 Khởi động Database Manager...")
    manager = DatabaseManager()
    manager.run()

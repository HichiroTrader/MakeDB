#!/usr/bin/env python3
"""
R|Trader Pro Control Center
Tool quản lý tập trung cho trader - Dễ sử dụng!
"""

import os
import sys
import json
import subprocess
import psycopg2
import redis
from datetime import datetime
from tabulate import tabulate
import time

class RTraderControlCenter:
    def __init__(self):
        self.env_file = '.env'
        self.config = self.load_config()
        
    def load_config(self):
        """Load configuration from .env file"""
        config = {}
        if os.path.exists(self.env_file):
            with open(self.env_file, 'r') as f:
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        config[key] = value
        return config
    
    def save_config(self):
        """Save configuration back to .env file"""
        lines = []
        
        # Read existing file preserving comments
        if os.path.exists(self.env_file):
            with open(self.env_file, 'r') as f:
                for line in f:
                    if line.startswith('#') or not line.strip():
                        lines.append(line)
                    elif '=' in line:
                        key = line.split('=')[0]
                        if key in self.config:
                            lines.append(f"{key}={self.config[key]}\n")
                        else:
                            lines.append(line)
        
        # Write back
        with open(self.env_file, 'w') as f:
            f.writelines(lines)
    
    def clear_screen(self):
        """Clear console screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def show_header(self):
        """Show application header"""
        self.clear_screen()
        print("="*70)
        print("               R|TRADER PRO CONTROL CENTER v2.0")
        print("                  Tool Quản Lý Tập Trung")
        print("="*70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        print()
    
    def show_main_menu(self):
        """Show main menu"""
        self.show_header()
        
        # Check system status
        docker_status = self.check_docker_status()
        db_status = self.check_db_status()
        collector_status = self.check_collector_status()
        
        print("📊 TRẠNG THÁI HỆ THỐNG:")
        print(f"   Docker Desktop: {docker_status}")
        print(f"   Database: {db_status}")
        print(f"   Collector: {collector_status}")
        print()
        
        print("📋 MENU CHÍNH:")
        print("="*70)
        print("1. 🚀 Khởi động hệ thống")
        print("2. 🛑 Dừng hệ thống")
        print("3. 🔄 Restart hệ thống")
        print("4. 📊 Quản lý Database")
        print("5. 🔧 Cấu hình hệ thống")
        print("6. 📈 Quản lý Symbols")
        print("7. 👤 Đổi account Rithmic")
        print("8. 🔌 Đổi port R|Trader Pro")
        print("9. 📋 Xem logs")
        print("10. 📊 Xem thống kê real-time")
        print("11. 💾 Backup/Restore")
        print("12. 📤 Export dữ liệu")
        print("0. ❌ Thoát")
        print("="*70)
    
    def check_docker_status(self):
        """Check Docker Desktop status"""
        try:
            result = subprocess.run(['docker', 'version'], capture_output=True, text=True)
            return "✅ Running" if result.returncode == 0 else "❌ Stopped"
        except:
            return "❌ Not installed"
    
    def check_db_status(self):
        """Check database status"""
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='rithmic_db',
                user='postgres',
                password='postgres'
            )
            conn.close()
            return "✅ Connected"
        except:
            return "❌ Disconnected"
    
    def check_collector_status(self):
        """Check collector status"""
        try:
            result = subprocess.run(['docker', 'ps', '--filter', 'name=rithmic_collector', '--format', '{{.Status}}'], 
                                  capture_output=True, text=True)
            if 'Up' in result.stdout:
                return "✅ Running"
            return "❌ Stopped"
        except:
            return "❌ Unknown"
    
    def start_system(self):
        """Start entire system"""
        self.show_header()
        print("🚀 KHỞI ĐỘNG HỆ THỐNG\n")
        
        # Check Docker
        print("1️⃣ Kiểm tra Docker Desktop...")
        if self.check_docker_status() == "❌ Stopped":
            print("   ⏳ Đang khởi động Docker Desktop...")
            os.startfile("C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe")
            print("   ⏳ Đợi 30 giây...")
            time.sleep(30)
        
        # Start containers
        print("\n2️⃣ Khởi động containers...")
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        result = subprocess.run(['docker-compose', 'up', '-d'], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("   ✅ Containers đã khởi động!")
        else:
            print(f"   ❌ Lỗi: {result.stderr}")
        
        # Check R|Trader Pro
        print("\n3️⃣ Kiểm tra R|Trader Pro...")
        rtrader_port = self.config.get('RTRADER_PORT', '3012')
        print(f"   ℹ️  R|Trader Pro cần chạy trên port {rtrader_port}")
        print("   ℹ️  Hãy đảm bảo R|Trader Pro đã được mở và Plugin Mode enabled")
        
        input("\n✅ Nhấn Enter để tiếp tục...")
    
    def stop_system(self):
        """Stop entire system"""
        self.show_header()
        print("🛑 DỪNG HỆ THỐNG\n")
        
        confirm = input("⚠️  Bạn chắc chắn muốn dừng hệ thống? (y/N): ")
        if confirm.lower() != 'y':
            return
        
        print("\n⏳ Đang dừng containers...")
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        result = subprocess.run(['docker-compose', 'down'], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Đã dừng hệ thống!")
        else:
            print(f"❌ Lỗi: {result.stderr}")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def restart_system(self):
        """Restart system"""
        self.show_header()
        print("🔄 RESTART HỆ THỐNG\n")
        
        print("1️⃣ Đang dừng containers...")
        subprocess.run(['docker-compose', 'down'])
        
        print("\n2️⃣ Đang khởi động lại...")
        subprocess.run(['docker-compose', 'up', '-d'])
        
        print("\n✅ Đã restart hệ thống!")
        input("\nNhấn Enter để tiếp tục...")
    
    def manage_database(self):
        """Launch database manager"""
        self.show_header()
        print("📊 QUẢN LÝ DATABASE\n")
        
        # Run db_manager.py
        subprocess.run([sys.executable, 'db_manager.py'])
    
    def configure_system(self):
        """System configuration menu"""
        while True:
            self.show_header()
            print("🔧 CẤU HÌNH HỆ THỐNG\n")
            
            print("Cấu hình hiện tại:")
            print("-"*70)
            print(f"R|Trader Pro Host: {self.config.get('RTRADER_HOST', 'host.docker.internal')}")
            print(f"R|Trader Pro Port: {self.config.get('RTRADER_PORT', '3012')}")
            print(f"Rithmic User: {self.config.get('RITHMIC_USER', 'Not set')}")
            print(f"Symbols: {self.config.get('SYMBOLS', 'Not set')}")
            print(f"Log Level: {self.config.get('LOG_LEVEL', 'INFO')}")
            print("-"*70)
            
            print("\n1. Sửa R|Trader Pro Host")
            print("2. Sửa R|Trader Pro Port")
            print("3. Sửa Log Level")
            print("4. Xem toàn bộ config")
            print("0. Quay lại")
            
            choice = input("\n👉 Chọn: ")
            
            if choice == '0':
                break
            elif choice == '1':
                new_host = input("Nhập host mới (mặc định: host.docker.internal): ")
                if new_host:
                    self.config['RTRADER_HOST'] = new_host
                    self.save_config()
                    print("✅ Đã cập nhật host!")
            elif choice == '2':
                new_port = input("Nhập port mới (mặc định: 3012): ")
                if new_port:
                    self.config['RTRADER_PORT'] = new_port
                    self.save_config()
                    print("✅ Đã cập nhật port!")
            elif choice == '3':
                print("\nLog levels: DEBUG, INFO, WARNING, ERROR")
                new_level = input("Nhập log level: ").upper()
                if new_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
                    self.config['LOG_LEVEL'] = new_level
                    self.save_config()
                    print("✅ Đã cập nhật log level!")
            elif choice == '4':
                print("\n📋 Toàn bộ configuration:")
                for key, value in sorted(self.config.items()):
                    if 'PASSWORD' in key:
                        print(f"{key}: {'*' * len(value)}")
                    else:
                        print(f"{key}: {value}")
            
            if choice in ['1', '2', '3']:
                restart = input("\n🔄 Restart collector để áp dụng? (y/N): ")
                if restart.lower() == 'y':
                    subprocess.run(['docker-compose', 'restart', 'collector'])
                    print("✅ Đã restart collector!")
            
            input("\nNhấn Enter để tiếp tục...")
    
    def manage_symbols(self):
        """Manage trading symbols"""
        while True:
            self.show_header()
            print("📈 QUẢN LÝ SYMBOLS\n")
            
            # Show current symbols
            current_symbols = self.config.get('SYMBOLS', '').split(',')
            print("Symbols hiện tại:")
            print("-"*70)
            for i, symbol in enumerate(current_symbols, 1):
                print(f"{i}. {symbol.strip()}")
            print("-"*70)
            
            print("\n1. Thêm symbol mới")
            print("2. Xóa symbol")
            print("3. Thay thế toàn bộ symbols")
            print("4. Xem symbols trong database")
            print("0. Quay lại")
            
            choice = input("\n👉 Chọn: ")
            
            if choice == '0':
                break
            elif choice == '1':
                new_symbol = input("Nhập symbol mới (VD: ESH5): ").upper()
                if new_symbol:
                    current_symbols.append(new_symbol)
                    self.config['SYMBOLS'] = ','.join(s.strip() for s in current_symbols)
                    self.save_config()
                    print(f"✅ Đã thêm {new_symbol}!")
                    
                    # Add to database
                    self.add_symbol_to_db(new_symbol)
            elif choice == '2':
                symbol_to_remove = input("Nhập symbol cần xóa: ").upper()
                if symbol_to_remove in [s.strip() for s in current_symbols]:
                    current_symbols = [s for s in current_symbols if s.strip() != symbol_to_remove]
                    self.config['SYMBOLS'] = ','.join(s.strip() for s in current_symbols)
                    self.save_config()
                    print(f"✅ Đã xóa {symbol_to_remove}!")
            elif choice == '3':
                new_symbols = input("Nhập danh sách symbols mới (cách nhau bằng dấu phẩy): ")
                if new_symbols:
                    self.config['SYMBOLS'] = new_symbols.upper()
                    self.save_config()
                    print("✅ Đã cập nhật danh sách symbols!")
            elif choice == '4':
                self.show_db_symbols()
            
            input("\nNhấn Enter để tiếp tục...")
    
    def add_symbol_to_db(self, symbol):
        """Add symbol to database and notify collector"""
        try:
            # Connect to Redis
            r = redis.Redis(host='localhost', port=6379)
            r.rpush('symbol_subscriptions', json.dumps({
                'symbol': symbol,
                'exchange': 'CME'
            }))
            print(f"📤 Đã gửi yêu cầu subscribe {symbol} đến collector!")
        except Exception as e:
            print(f"⚠️  Không thể kết nối Redis: {e}")
    
    def show_db_symbols(self):
        """Show symbols from database"""
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='rithmic_db',
                user='postgres',
                password='postgres'
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT s.symbol, s.exchange, s.active, 
                       COUNT(t.id) as tick_count
                FROM symbols s
                LEFT JOIN tick_data t ON s.symbol = t.symbol
                GROUP BY s.symbol, s.exchange, s.active
                ORDER BY s.symbol
            """)
            
            data = cursor.fetchall()
            if data:
                print("\n📊 Symbols trong database:")
                headers = ["Symbol", "Exchange", "Active", "Ticks"]
                formatted_data = []
                for row in data:
                    formatted_data.append([
                        row[0], row[1], 
                        "✅" if row[2] else "❌",
                        f"{row[3]:,}" if row[3] else "0"
                    ])
                print(tabulate(formatted_data, headers=headers, tablefmt="grid"))
            
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ Lỗi: {e}")
    
    def change_rithmic_account(self):
        """Change Rithmic account"""
        self.show_header()
        print("👤 ĐỔI ACCOUNT RITHMIC\n")
        
        print("Account hiện tại:")
        print(f"Username: {self.config.get('RITHMIC_USER', 'Not set')}")
        print(f"Password: {'*' * len(self.config.get('RITHMIC_PASSWORD', ''))}")
        
        print("\n⚠️  Lưu ý: Thay đổi account sẽ cần restart collector")
        
        new_user = input("\nNhập username mới (hoặc Enter để giữ nguyên): ")
        new_pass = input("Nhập password mới (hoặc Enter để giữ nguyên): ")
        
        if new_user:
            self.config['RITHMIC_USER'] = new_user
        if new_pass:
            self.config['RITHMIC_PASSWORD'] = new_pass
        
        if new_user or new_pass:
            self.save_config()
            print("\n✅ Đã cập nhật account!")
            
            restart = input("🔄 Restart collector ngay? (y/N): ")
            if restart.lower() == 'y':
                subprocess.run(['docker-compose', 'restart', 'collector'])
                print("✅ Đã restart collector!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def change_rtrader_port(self):
        """Change R|Trader Pro port"""
        self.show_header()
        print("🔌 ĐỔI PORT R|TRADER PRO\n")
        
        current_port = self.config.get('RTRADER_PORT', '3012')
        print(f"Port hiện tại: {current_port}")
        
        print("\n📝 Hướng dẫn:")
        print("1. Mở R|Trader Pro")
        print("2. Vào Settings → Plugin Configuration")
        print("3. Xem Plugin Port")
        
        new_port = input("\nNhập port mới: ")
        if new_port and new_port != current_port:
            self.config['RTRADER_PORT'] = new_port
            self.save_config()
            print(f"\n✅ Đã đổi port thành {new_port}!")
            
            restart = input("🔄 Restart collector ngay? (y/N): ")
            if restart.lower() == 'y':
                subprocess.run(['docker-compose', 'restart', 'collector'])
                print("✅ Đã restart collector!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def view_logs(self):
        """View system logs"""
        while True:
            self.show_header()
            print("📋 XEM LOGS\n")
            
            print("1. Logs của Collector")
            print("2. Logs của API")
            print("3. Logs của Database")
            print("4. Logs của tất cả services")
            print("0. Quay lại")
            
            choice = input("\n👉 Chọn: ")
            
            if choice == '0':
                break
            elif choice == '1':
                print("\n📋 Collector logs (Nhấn Ctrl+C để thoát):\n")
                subprocess.run(['docker-compose', 'logs', '-f', '--tail=50', 'collector'])
            elif choice == '2':
                print("\n📋 API logs (Nhấn Ctrl+C để thoát):\n")
                subprocess.run(['docker-compose', 'logs', '-f', '--tail=50', 'api'])
            elif choice == '3':
                print("\n📋 Database logs (Nhấn Ctrl+C để thoát):\n")
                subprocess.run(['docker-compose', 'logs', '-f', '--tail=50', 'db'])
            elif choice == '4':
                print("\n📋 All logs (Nhấn Ctrl+C để thoát):\n")
                subprocess.run(['docker-compose', 'logs', '-f', '--tail=20'])
            
            input("\nNhấn Enter để tiếp tục...")
    
    def show_realtime_stats(self):
        """Show real-time statistics"""
        self.show_header()
        print("📊 THỐNG KÊ REAL-TIME\n")
        
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='rithmic_db',
                user='postgres',
                password='postgres'
            )
            
            while True:
                self.clear_screen()
                print("📊 THỐNG KÊ REAL-TIME (Nhấn Ctrl+C để thoát)")
                print("="*70)
                print(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*70)
                
                cursor = conn.cursor()
                
                # Tick data stats
                cursor.execute("""
                    SELECT symbol, 
                           COUNT(*) as ticks_1min,
                           MAX(price) as last_price,
                           MAX(timestamp) as last_update
                    FROM tick_data
                    WHERE timestamp > NOW() - INTERVAL '1 minute'
                    GROUP BY symbol
                    ORDER BY symbol
                """)
                
                tick_data = cursor.fetchall()
                if tick_data:
                    print("\n📈 TICK DATA (1 phút gần nhất):")
                    headers = ["Symbol", "Ticks/min", "Last Price", "Last Update"]
                    formatted_data = []
                    for row in tick_data:
                        formatted_data.append([
                            row[0], 
                            row[1],
                            f"{row[2]:.2f}" if row[2] else "N/A",
                            row[3].strftime("%H:%M:%S") if row[3] else "N/A"
                        ])
                    print(tabulate(formatted_data, headers=headers, tablefmt="grid"))
                
                # Level 2 stats
                cursor.execute("""
                    SELECT symbol, COUNT(DISTINCT level) as depth_levels
                    FROM level2_data
                    WHERE timestamp > NOW() - INTERVAL '10 seconds'
                    GROUP BY symbol
                """)
                
                level2_data = cursor.fetchall()
                if level2_data:
                    print("\n📊 LEVEL 2 DEPTH:")
                    for symbol, depth in level2_data:
                        print(f"   {symbol}: {depth} levels")
                
                cursor.close()
                time.sleep(2)  # Update every 2 seconds
                
        except KeyboardInterrupt:
            print("\n\n✅ Đã dừng thống kê")
        except Exception as e:
            print(f"❌ Lỗi: {e}")
        finally:
            if conn:
                conn.close()
        
        input("\nNhấn Enter để tiếp tục...")
    
    def backup_restore_menu(self):
        """Backup and restore menu"""
        while True:
            self.show_header()
            print("💾 BACKUP/RESTORE\n")
            
            print("1. 📦 Backup database")
            print("2. 📂 Restore database")
            print("3. 📋 Xem danh sách backup")
            print("4. 🗑️  Xóa backup cũ")
            print("0. Quay lại")
            
            choice = input("\n👉 Chọn: ")
            
            if choice == '0':
                break
            elif choice == '1':
                self.backup_database()
            elif choice == '2':
                self.restore_database()
            elif choice == '3':
                self.list_backups()
            elif choice == '4':
                self.delete_old_backups()
            
            input("\nNhấn Enter để tiếp tục...")
    
    def backup_database(self):
        """Backup database"""
        print("\n📦 BACKUP DATABASE\n")
        
        backup_name = input("Tên backup (Enter để dùng timestamp): ")
        if not backup_name:
            backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        os.makedirs("backups", exist_ok=True)
        backup_file = f"backups/{backup_name}.sql"
        
        print(f"\n⏳ Đang backup vào {backup_file}...")
        
        cmd = f'docker-compose exec -T db pg_dump -U postgres rithmic_db > {backup_file}'
        result = subprocess.run(cmd, shell=True, capture_output=True)
        
        if result.returncode == 0 and os.path.exists(backup_file):
            size = os.path.getsize(backup_file) / (1024 * 1024)
            print(f"✅ Backup thành công! Size: {size:.2f} MB")
        else:
            print("❌ Backup thất bại!")
    
    def restore_database(self):
        """Restore database"""
        print("\n📂 RESTORE DATABASE\n")
        
        # List backups
        if not os.path.exists("backups"):
            print("❌ Không có backup nào!")
            return
        
        backups = [f for f in os.listdir("backups") if f.endswith('.sql')]
        if not backups:
            print("❌ Không có backup nào!")
            return
        
        print("Danh sách backup:")
        for i, backup in enumerate(backups, 1):
            size = os.path.getsize(f"backups/{backup}") / (1024 * 1024)
            print(f"{i}. {backup} ({size:.2f} MB)")
        
        choice = input("\nChọn số backup: ")
        try:
            backup_file = f"backups/{backups[int(choice) - 1]}"
        except:
            print("❌ Lựa chọn không hợp lệ!")
            return
        
        confirm = input(f"\n⚠️  Restore sẽ XÓA dữ liệu hiện tại! Tiếp tục? (y/N): ")
        if confirm.lower() != 'y':
            return
        
        print(f"\n⏳ Đang restore từ {backup_file}...")
        cmd = f'docker-compose exec -T db psql -U postgres rithmic_db < {backup_file}'
        result = subprocess.run(cmd, shell=True)
        
        if result.returncode == 0:
            print("✅ Restore thành công!")
        else:
            print("❌ Restore thất bại!")
    
    def list_backups(self):
        """List all backups"""
        print("\n📋 DANH SÁCH BACKUP\n")
        
        if not os.path.exists("backups"):
            print("❌ Chưa có backup nào!")
            return
        
        backups = [f for f in os.listdir("backups") if f.endswith('.sql')]
        if not backups:
            print("❌ Chưa có backup nào!")
            return
        
        total_size = 0
        print(f"{'Tên file':<40} {'Kích thước':>10} {'Ngày tạo':>20}")
        print("-"*70)
        
        for backup in sorted(backups):
            filepath = f"backups/{backup}"
            size = os.path.getsize(filepath) / (1024 * 1024)
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
            total_size += size
            
            print(f"{backup:<40} {size:>8.2f} MB {mtime.strftime('%Y-%m-%d %H:%M'):>20}")
        
        print("-"*70)
        print(f"{'Tổng cộng:':<40} {total_size:>8.2f} MB")
    
    def delete_old_backups(self):
        """Delete old backups"""
        print("\n🗑️  XÓA BACKUP CŨ\n")
        
        days = input("Xóa backup cũ hơn bao nhiêu ngày? (mặc định 30): ")
        days = int(days) if days else 30
        
        if not os.path.exists("backups"):
            print("❌ Không có backup nào!")
            return
        
        deleted = 0
        cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)
        
        for backup in os.listdir("backups"):
            if backup.endswith('.sql'):
                filepath = f"backups/{backup}"
                if os.path.getmtime(filepath) < cutoff_time:
                    os.remove(filepath)
                    deleted += 1
                    print(f"🗑️  Đã xóa {backup}")
        
        print(f"\n✅ Đã xóa {deleted} backup cũ!")
    
    def export_data(self):
        """Export data menu"""
        self.show_header()
        print("📤 EXPORT DỮ LIỆU\n")
        
        print("1. Export tick data")
        print("2. Export level 2 data")
        print("3. Export volume profile")
        print("4. Export tất cả")
        
        choice = input("\n👉 Chọn: ")
        
        symbol = input("Symbol (Enter để export tất cả): ").upper()
        days = input("Số ngày (mặc định 1): ")
        days = int(days) if days else 1
        
        os.makedirs("exports", exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='rithmic_db',
                user='postgres',
                password='postgres'
            )
            
            if choice in ['1', '4']:
                # Export tick data
                query = "SELECT * FROM tick_data WHERE timestamp > NOW() - INTERVAL '%s days'"
                params = [days]
                if symbol:
                    query += " AND symbol = %s"
                    params.append(symbol)
                query += " ORDER BY timestamp"
                
                df = pd.read_sql(query, conn, params=params)
                if not df.empty:
                    filename = f"exports/ticks_{symbol if symbol else 'all'}_{timestamp}.csv"
                    df.to_csv(filename, index=False)
                    print(f"✅ Exported {len(df)} tick records to {filename}")
            
            if choice in ['2', '4']:
                # Export level 2 data
                query = "SELECT * FROM level2_data WHERE timestamp > NOW() - INTERVAL '%s days'"
                params = [days]
                if symbol:
                    query += " AND symbol = %s"
                    params.append(symbol)
                query += " ORDER BY timestamp, side, level"
                
                df = pd.read_sql(query, conn, params=params)
                if not df.empty:
                    filename = f"exports/level2_{symbol if symbol else 'all'}_{timestamp}.csv"
                    df.to_csv(filename, index=False)
                    print(f"✅ Exported {len(df)} level2 records to {filename}")
            
            if choice in ['3', '4']:
                # Export volume profile
                query = """
                    SELECT symbol, price_level, 
                           SUM(buy_volume) as buy_vol,
                           SUM(sell_volume) as sell_vol,
                           SUM(buy_volume - sell_volume) as delta
                    FROM volume_profile
                    WHERE timestamp > NOW() - INTERVAL '%s days'
                """
                params = [days]
                if symbol:
                    query += " AND symbol = %s"
                    params.append(symbol)
                query += " GROUP BY symbol, price_level ORDER BY symbol, price_level"
                
                df = pd.read_sql(query, conn, params=params)
                if not df.empty:
                    filename = f"exports/volume_{symbol if symbol else 'all'}_{timestamp}.csv"
                    df.to_csv(filename, index=False)
                    print(f"✅ Exported volume profile to {filename}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Lỗi: {e}")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def run(self):
        """Main application loop"""
        while True:
            self.show_main_menu()
            
            choice = input("\n👉 Chọn chức năng: ")
            
            try:
                if choice == '0':
                    print("\n👋 Cảm ơn đã sử dụng R|Trader Pro Control Center!")
                    break
                elif choice == '1':
                    self.start_system()
                elif choice == '2':
                    self.stop_system()
                elif choice == '3':
                    self.restart_system()
                elif choice == '4':
                    self.manage_database()
                elif choice == '5':
                    self.configure_system()
                elif choice == '6':
                    self.manage_symbols()
                elif choice == '7':
                    self.change_rithmic_account()
                elif choice == '8':
                    self.change_rtrader_port()
                elif choice == '9':
                    self.view_logs()
                elif choice == '10':
                    self.show_realtime_stats()
                elif choice == '11':
                    self.backup_restore_menu()
                elif choice == '12':
                    self.export_data()
                else:
                    print("\n❌ Lựa chọn không hợp lệ!")
                    input("Nhấn Enter để tiếp tục...")
                    
            except KeyboardInterrupt:
                print("\n\n⚠️  Đã hủy thao tác")
                input("Nhấn Enter để tiếp tục...")
            except Exception as e:
                print(f"\n❌ Lỗi: {e}")
                input("Nhấn Enter để tiếp tục...")


if __name__ == "__main__":
    print("🚀 Đang khởi động R|Trader Pro Control Center...")
    
    # Check if pandas is installed
    try:
        import pandas as pd
    except ImportError:
        print("⚠️  Cần cài đặt pandas: pip install pandas")
        sys.exit(1)
    
    # Run the control center
    control_center = RTraderControlCenter()
    control_center.run()
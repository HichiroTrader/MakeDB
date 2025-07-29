#!/usr/bin/env python3
"""
Script để thay đổi account Rithmic
"""
import os
import fileinput
import sys

def change_account():
    """Thay đổi thông tin account Rithmic"""
    print("=" * 60)
    print("THAY ĐỔI ACCOUNT RITHMIC")
    print("=" * 60)
    
    # Nhập thông tin mới
    new_user = input("Email/Username mới: ")
    new_password = input("Password mới: ")
    
    # Xác nhận
    print(f"\n📋 Thông tin mới:")
    print(f"Username: {new_user}")
    print(f"Password: {'*' * len(new_password)}")
    
    confirm = input("\n✅ Xác nhận thay đổi? (y/N): ")
    if confirm.lower() != 'y':
        print("❌ Đã hủy!")
        return
    
    # Đọc file .env
    env_file = '.env'
    with open(env_file, 'r') as f:
        lines = f.readlines()
    
    # Thay đổi thông tin
    with open(env_file, 'w') as f:
        for line in lines:
            if line.startswith('RITHMIC_USER='):
                f.write(f'RITHMIC_USER={new_user}\n')
            elif line.startswith('RITHMIC_PASSWORD='):
                f.write(f'RITHMIC_PASSWORD={new_password}\n')
            else:
                f.write(line)
    
    print("✅ Đã cập nhật file .env!")
    
    # Restart collector
    restart = input("\n🔄 Restart collector để áp dụng? (y/N): ")
    if restart.lower() == 'y':
        os.system('docker-compose restart collector')
        print("✅ Đã restart collector!")
    else:
        print("⚠️  Nhớ restart collector để áp dụng thay đổi:")
        print("   docker-compose restart collector")

if __name__ == "__main__":
    change_account()
#!/usr/bin/env python3
"""
Test R|Trader Pro Binary Protocol Connection
Tests all 4 ports and verifies data flow
"""

import socket
import struct
import time
import asyncio

class RithmicPortTester:
    def __init__(self):
        self.ports = {
            3010: "Ticker Plant (Market Data)",
            3011: "Order Plant (Trading)",
            3012: "PNL Plant (Positions)",
            3013: "History Plant (Historical Data)"
        }
        
    def test_tcp_connection(self, host='localhost', port=3010):
        """Test basic TCP connection"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except:
            return False
    
    async def test_binary_protocol(self, host='localhost', port=3010):
        """Test Rithmic binary protocol"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=5.0
            )
            
            print(f"✅ Connected to {host}:{port}")
            
            # Try to read any incoming data (non-blocking)
            try:
                data = await asyncio.wait_for(reader.read(1024), timeout=2.0)
                if data:
                    print(f"📥 Received {len(data)} bytes")
                    # Try to parse as Rithmic format
                    if len(data) >= 4:
                        msg_len = struct.unpack('>I', data[0:4])[0]
                        print(f"   Message length: {msg_len}")
            except asyncio.TimeoutError:
                print("⏱️  No immediate data (normal for binary protocol)")
            
            # Close connection
            writer.close()
            await writer.wait_closed()
            return True
            
        except Exception as e:
            print(f"❌ Binary protocol test failed: {e}")
            return False
    
    def test_all_ports(self, host='localhost'):
        """Test all Rithmic ports"""
        print("="*60)
        print("R|TRADER PRO PORT TEST")
        print("="*60)
        print(f"Testing host: {host}\n")
        
        results = {}
        
        for port, description in self.ports.items():
            print(f"Testing port {port} ({description})...")
            
            # Test TCP connection
            tcp_ok = self.test_tcp_connection(host, port)
            
            if tcp_ok:
                print(f"  ✅ TCP connection successful")
                results[port] = True
            else:
                print(f"  ❌ TCP connection failed")
                results[port] = False
            
            print()
        
        # Summary
        print("="*60)
        print("SUMMARY:")
        print("="*60)
        active_ports = [p for p, ok in results.items() if ok]
        if active_ports:
            print(f"✅ Active ports: {', '.join(map(str, active_ports))}")
        else:
            print("❌ No active ports found!")
        
        return results
    
    async def test_data_flow(self, host='localhost', port=3010):
        """Test actual data flow"""
        print("\n" + "="*60)
        print(f"TESTING DATA FLOW ON PORT {port}")
        print("="*60)
        
        await self.test_binary_protocol(host, port)
    
    def diagnose_connection_issues(self):
        """Diagnose common connection issues"""
        print("\n" + "="*60)
        print("DIAGNOSTICS")
        print("="*60)
        
        # Check if R|Trader Pro process is running
        import subprocess
        try:
            result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq Rithmic Trader Pro.exe'], 
                                  capture_output=True, text=True)
            if 'Rithmic Trader Pro.exe' in result.stdout:
                print("✅ R|Trader Pro process is running")
            else:
                print("❌ R|Trader Pro process NOT found!")
                print("   Please start R|Trader Pro first")
        except:
            print("⚠️  Could not check process status")
        
        # Check netstat for listening ports
        try:
            result = subprocess.run(['netstat', '-an'], capture_output=True, text=True)
            for port in [3010, 3011, 3012, 3013]:
                if f":{port}" in result.stdout and "LISTENING" in result.stdout:
                    print(f"✅ Port {port} is LISTENING")
                elif f":{port}" in result.stdout:
                    print(f"⚠️  Port {port} found but not LISTENING")
                else:
                    print(f"❌ Port {port} not found in netstat")
        except:
            print("⚠️  Could not check netstat")
        
        print("\n📝 Recommendations:")
        print("1. Ensure R|Trader Pro is running")
        print("2. Check R|Trader Pro settings for Plugin Mode")
        print("3. Verify no firewall blocking ports 3010-3013")
        print("4. Try connecting as Administrator")
        print("5. Check if another application uses these ports")


async def main():
    tester = RithmicPortTester()
    
    # Test localhost
    print("\n🔍 Testing localhost connections...")
    local_results = tester.test_all_ports('localhost')
    
    # Test 127.0.0.1 
    print("\n\n🔍 Testing 127.0.0.1 connections...")
    ip_results = tester.test_all_ports('127.0.0.1')
    
    # Find active port
    active_port = None
    for port in [3010, 3013, 3011, 3012]:
        if local_results.get(port) or ip_results.get(port):
            active_port = port
            break
    
    if active_port:
        print(f"\n\n🎯 Testing data flow on active port {active_port}...")
        await tester.test_data_flow('localhost', active_port)
    
    # Run diagnostics
    tester.diagnose_connection_issues()


if __name__ == "__main__":
    print("🚀 R|Trader Pro Connection Tester")
    print("="*60)
    asyncio.run(main())

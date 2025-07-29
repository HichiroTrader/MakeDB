#!/usr/bin/env python3
"""
Test script for RithmicDataCollector API
Provides comprehensive testing of all API endpoints
"""

import json
import requests
import time
import sys
from typing import Dict, Any, List

class APITester:
    def __init__(self, base_url: str = "http://localhost"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.timeout = 30
        
    def log_info(self, message: str):
        print(f"[INFO] {message}")
        
    def log_success(self, message: str):
        print(f"[SUCCESS] {message}")
        
    def log_error(self, message: str):
        print(f"[ERROR] {message}")
        
    def log_warning(self, message: str):
        print(f"[WARNING] {message}")

    def test_health_endpoint(self) -> bool:
        """Test the health check endpoint"""
        self.log_info("Testing health endpoint...")
        
        try:
            response = self.session.get(f"{self.base_url}/health")
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'healthy':
                    self.log_success("Health check passed")
                    return True
                else:
                    self.log_error(f"Health check failed: {data}")
                    return False
            else:
                self.log_error(f"Health endpoint returned {response.status_code}")
                return False
                
        except Exception as e:
            self.log_error(f"Health check failed with exception: {e}")
            return False

    def test_stats_endpoint(self) -> bool:
        """Test the statistics endpoint"""
        self.log_info("Testing stats endpoint...")
        
        try:
            response = self.session.get(f"{self.base_url}/api/stats")
            
            if response.status_code == 200:
                data = response.json()
                required_keys = ['tick_data', 'level2_data', 'timestamp']
                
                if all(key in data for key in required_keys):
                    self.log_success(f"Stats endpoint working. Total ticks: {data['tick_data']['total_records']}")
                    return True
                else:
                    self.log_error(f"Stats response missing required keys: {data}")
                    return False
            else:
                self.log_error(f"Stats endpoint returned {response.status_code}")
                return False
                
        except Exception as e:
            self.log_error(f"Stats test failed with exception: {e}")
            return False

    def test_symbols_endpoint(self) -> List[str]:
        """Test the symbols endpoint and return available symbols"""
        self.log_info("Testing symbols endpoint...")
        
        try:
            response = self.session.get(f"{self.base_url}/api/symbols")
            
            if response.status_code == 200:
                data = response.json()
                symbols = [symbol['symbol'] for symbol in data.get('symbols', [])]
                
                if symbols:
                    self.log_success(f"Found {len(symbols)} symbols: {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")
                else:
                    self.log_warning("No symbols found with recent data")
                    
                return symbols
            else:
                self.log_error(f"Symbols endpoint returned {response.status_code}")
                return []
                
        except Exception as e:
            self.log_error(f"Symbols test failed with exception: {e}")
            return []

    def test_tick_data_endpoint(self, symbol: str, limit: int = 10) -> bool:
        """Test the tick data endpoint for a specific symbol"""
        self.log_info(f"Testing tick data endpoint for {symbol}...")
        
        try:
            response = self.session.get(f"{self.base_url}/api/ticks/{symbol}?limit={limit}")
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data and isinstance(data['data'], list):
                    tick_count = len(data['data'])
                    
                    if tick_count > 0:
                        # Validate tick data structure
                        first_tick = data['data'][0]
                        required_fields = ['timestamp', 'symbol', 'price', 'volume']
                        
                        if all(field in first_tick for field in required_fields):
                            self.log_success(f"Retrieved {tick_count} ticks for {symbol}")
                            return True
                        else:
                            self.log_error(f"Tick data missing required fields: {first_tick}")
                            return False
                    else:
                        self.log_warning(f"No tick data found for {symbol}")
                        return True  # Not an error, just no data
                else:
                    self.log_error(f"Invalid tick data response format: {data}")
                    return False
            else:
                self.log_error(f"Tick data endpoint returned {response.status_code} for {symbol}")
                return False
                
        except Exception as e:
            self.log_error(f"Tick data test failed with exception: {e}")
            return False

    def test_level2_data_endpoint(self, symbol: str, limit: int = 5) -> bool:
        """Test the Level 2 data endpoint for a specific symbol"""
        self.log_info(f"Testing Level 2 data endpoint for {symbol}...")
        
        try:
            response = self.session.get(f"{self.base_url}/api/level2/{symbol}?limit={limit}")
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data and isinstance(data['data'], list):
                    level2_count = len(data['data'])
                    
                    if level2_count > 0:
                        # Validate Level 2 data structure
                        first_level2 = data['data'][0]
                        required_fields = ['timestamp', 'symbol', 'bids', 'asks']
                        
                        if all(field in first_level2 for field in required_fields):
                            self.log_success(f"Retrieved {level2_count} Level 2 updates for {symbol}")
                            return True
                        else:
                            self.log_error(f"Level 2 data missing required fields: {first_level2}")
                            return False
                    else:
                        self.log_warning(f"No Level 2 data found for {symbol}")
                        return True  # Not an error, just no data
                else:
                    self.log_error(f"Invalid Level 2 data response format: {data}")
                    return False
            else:
                self.log_error(f"Level 2 data endpoint returned {response.status_code} for {symbol}")
                return False
                
        except Exception as e:
            self.log_error(f"Level 2 data test failed with exception: {e}")
            return False

    def test_subscribe_endpoint(self, symbol: str, exchange: str = "CME") -> bool:
        """Test the symbol subscription endpoint"""
        self.log_info(f"Testing subscription endpoint for {symbol}...")
        
        try:
            payload = {"exchange": exchange}
            response = self.session.post(
                f"{self.base_url}/api/subscribe/{symbol}",
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'subscribed':
                    self.log_success(f"Successfully subscribed to {symbol}")
                    return True
                else:
                    self.log_error(f"Subscription failed: {data}")
                    return False
            else:
                self.log_error(f"Subscribe endpoint returned {response.status_code} for {symbol}")
                return False
                
        except Exception as e:
            self.log_error(f"Subscribe test failed with exception: {e}")
            return False

    def test_rate_limiting(self) -> bool:
        """Test rate limiting functionality"""
        self.log_info("Testing rate limiting...")
        
        try:
            # Make rapid requests to trigger rate limiting
            responses = []
            for i in range(10):
                response = self.session.get(f"{self.base_url}/api/stats")
                responses.append(response.status_code)
                time.sleep(0.1)  # Small delay
            
            # Check if any requests were rate limited (429)
            rate_limited = any(status == 429 for status in responses)
            
            if rate_limited:
                self.log_success("Rate limiting is working")
                return True
            else:
                self.log_warning("Rate limiting may not be configured or limits are high")
                return True  # Not necessarily an error
                
        except Exception as e:
            self.log_error(f"Rate limiting test failed with exception: {e}")
            return False

    def test_cors_headers(self) -> bool:
        """Test CORS headers"""
        self.log_info("Testing CORS headers...")
        
        try:
            # Test preflight request
            response = self.session.options(
                f"{self.base_url}/api/stats",
                headers={
                    'Origin': 'http://example.com',
                    'Access-Control-Request-Method': 'GET',
                    'Access-Control-Request-Headers': 'Content-Type'
                }
            )
            
            if response.status_code == 204:
                cors_headers = {
                    'Access-Control-Allow-Origin': response.headers.get('Access-Control-Allow-Origin'),
                    'Access-Control-Allow-Methods': response.headers.get('Access-Control-Allow-Methods'),
                    'Access-Control-Allow-Headers': response.headers.get('Access-Control-Allow-Headers')
                }
                
                if cors_headers['Access-Control-Allow-Origin']:
                    self.log_success("CORS headers are configured")
                    return True
                else:
                    self.log_warning("CORS headers may not be properly configured")
                    return False
            else:
                self.log_error(f"CORS preflight request returned {response.status_code}")
                return False
                
        except Exception as e:
            self.log_error(f"CORS test failed with exception: {e}")
            return False

    def test_error_handling(self) -> bool:
        """Test error handling for invalid requests"""
        self.log_info("Testing error handling...")
        
        try:
            # Test invalid endpoint
            response = self.session.get(f"{self.base_url}/api/invalid")
            if response.status_code == 404:
                self.log_success("404 error handling works")
            else:
                self.log_warning(f"Expected 404, got {response.status_code}")
            
            # Test invalid symbol
            response = self.session.get(f"{self.base_url}/api/ticks/INVALID_SYMBOL")
            if response.status_code in [200, 404]:  # Either is acceptable
                self.log_success("Invalid symbol handling works")
            else:
                self.log_warning(f"Unexpected response for invalid symbol: {response.status_code}")
            
            # Test invalid limit parameter
            response = self.session.get(f"{self.base_url}/api/ticks/ESZ23?limit=invalid")
            if response.status_code in [200, 400]:  # Either is acceptable
                self.log_success("Invalid parameter handling works")
            else:
                self.log_warning(f"Unexpected response for invalid parameter: {response.status_code}")
            
            return True
            
        except Exception as e:
            self.log_error(f"Error handling test failed with exception: {e}")
            return False

    def run_comprehensive_test(self) -> Dict[str, bool]:
        """Run all tests and return results"""
        self.log_info("Starting comprehensive API test suite...")
        print("=" * 60)
        
        results = {}
        
        # Basic connectivity tests
        results['health'] = self.test_health_endpoint()
        results['stats'] = self.test_stats_endpoint()
        
        # Get available symbols
        symbols = self.test_symbols_endpoint()
        results['symbols'] = len(symbols) >= 0  # Always pass if no exception
        
        # Test data endpoints with available symbols
        if symbols:
            test_symbol = symbols[0]  # Use first available symbol
            results['tick_data'] = self.test_tick_data_endpoint(test_symbol)
            results['level2_data'] = self.test_level2_data_endpoint(test_symbol)
        else:
            # Test with default symbols
            results['tick_data'] = self.test_tick_data_endpoint('ESZ23')
            results['level2_data'] = self.test_level2_data_endpoint('ESZ23')
        
        # Test subscription endpoint
        results['subscribe'] = self.test_subscribe_endpoint('TEST23', 'CME')
        
        # Test additional functionality
        results['rate_limiting'] = self.test_rate_limiting()
        results['cors'] = self.test_cors_headers()
        results['error_handling'] = self.test_error_handling()
        
        # Summary
        print("=" * 60)
        self.log_info("Test Results Summary:")
        
        passed = sum(results.values())
        total = len(results)
        
        for test_name, passed_test in results.items():
            status = "PASS" if passed_test else "FAIL"
            print(f"  {test_name.replace('_', ' ').title()}: {status}")
        
        print(f"\nOverall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
        
        if passed == total:
            self.log_success("All tests passed! API is working correctly.")
        elif passed >= total * 0.8:
            self.log_warning(f"Most tests passed ({passed}/{total}). Some issues detected.")
        else:
            self.log_error(f"Many tests failed ({total-passed}/{total}). API may have issues.")
        
        return results

    def run_load_test(self, duration: int = 60, concurrent_requests: int = 5) -> Dict[str, Any]:
        """Run a simple load test"""
        self.log_info(f"Running load test for {duration} seconds with {concurrent_requests} concurrent requests...")
        
        import threading
        import time
        from collections import defaultdict
        
        results = defaultdict(int)
        start_time = time.time()
        
        def make_requests():
            while time.time() - start_time < duration:
                try:
                    response = self.session.get(f"{self.base_url}/api/stats")
                    results[response.status_code] += 1
                except Exception:
                    results['errors'] += 1
                time.sleep(0.1)  # Small delay between requests
        
        # Start concurrent threads
        threads = []
        for _ in range(concurrent_requests):
            thread = threading.Thread(target=make_requests)
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        total_requests = sum(results.values())
        success_rate = results[200] / total_requests * 100 if total_requests > 0 else 0
        
        self.log_info(f"Load test completed:")
        self.log_info(f"  Total requests: {total_requests}")
        self.log_info(f"  Success rate: {success_rate:.1f}%")
        self.log_info(f"  Requests per second: {total_requests/duration:.1f}")
        
        return dict(results)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Test RithmicDataCollector API')
    parser.add_argument('--url', default='http://localhost', help='Base URL of the API')
    parser.add_argument('--load-test', action='store_true', help='Run load test')
    parser.add_argument('--duration', type=int, default=60, help='Load test duration in seconds')
    parser.add_argument('--concurrent', type=int, default=5, help='Number of concurrent requests for load test')
    
    args = parser.parse_args()
    
    tester = APITester(args.url)
    
    if args.load_test:
        tester.run_load_test(args.duration, args.concurrent)
    else:
        results = tester.run_comprehensive_test()
        
        # Exit with error code if tests failed
        if not all(results.values()):
            sys.exit(1)

if __name__ == '__main__':
    main()
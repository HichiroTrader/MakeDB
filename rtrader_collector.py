#!/usr/bin/env python3
"""
R|Trader Pro Plugin Mode Data Collector
This collector connects to R|Trader Pro via Plugin API
"""

import asyncio
import json
import logging
import os
import sys
import socket
import struct
import threading
from datetime import datetime
from typing import Dict, List, Optional, Callable
from decimal import Decimal

import psycopg2
import psycopg2.extras
import redis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RTraderProPlugin:
    """R|Trader Pro Plugin API Client"""
    
    def __init__(self, host='localhost', port=3012):
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False
        self.callbacks = {}
        self.running = False
        
    def connect(self):
        """Connect to R|Trader Pro Plugin API"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(10)  # 10 second timeout
            self.socket.connect((self.host, self.port))
            self.connected = True
            self.running = True
            
            # Start listening thread
            listener_thread = threading.Thread(target=self._listen)
            listener_thread.daemon = True
            listener_thread.start()
            
            logger.info(f"Connected to R|Trader Pro at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to R|Trader Pro: {e}")
            return False
    
    def _listen(self):
        """Listen for incoming messages"""
        while self.running and self.connected:
            try:
                # Read message (R|Trader Pro typically sends line-delimited JSON)
                data = self.socket.recv(4096)
                if not data:
                    break
                
                # Process each line as a separate message
                messages = data.decode('utf-8').strip().split('\n')
                for msg_str in messages:
                    if msg_str:
                        try:
                            message = json.loads(msg_str)
                            self._process_message(message)
                        except json.JSONDecodeError as e:
                            logger.error(f"Invalid JSON message: {msg_str}, error: {e}")
                
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"Error in listener: {e}")
                break
        
        self.connected = False
        logger.info("R|Trader Pro listener stopped")
    
    def _process_message(self, message: Dict):
        """Process incoming message"""
        msg_type = message.get('type', '')
        
        if msg_type == 'TICK' or msg_type == 'TRADE':
            if 'tick_callback' in self.callbacks:
                self.callbacks['tick_callback'](message)
        elif msg_type == 'QUOTE' or msg_type == 'BBO':
            if 'quote_callback' in self.callbacks:
                self.callbacks['quote_callback'](message)
        elif msg_type == 'DEPTH' or msg_type == 'LEVEL2':
            if 'level2_callback' in self.callbacks:
                self.callbacks['level2_callback'](message)
        elif msg_type == 'STATUS':
            logger.info(f"Status: {message}")
        elif msg_type == 'ERROR':
            logger.error(f"Error from R|Trader Pro: {message}")
        else:
            logger.debug(f"Unknown message type: {msg_type}")
    
    def send_message(self, message: Dict):
        """Send message to R|Trader Pro"""
        if not self.connected:
            return False
        
        try:
            # Send as JSON with newline delimiter
            message_str = json.dumps(message) + '\n'
            self.socket.send(message_str.encode('utf-8'))
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def subscribe_market_data(self, symbol: str, exchange: str = 'CME'):
        """Subscribe to market data for a symbol"""
        message = {
            'action': 'SUBSCRIBE',
            'type': 'MARKET_DATA',
            'symbol': symbol,
            'exchange': exchange,
            'data_types': ['TRADE', 'QUOTE', 'DEPTH']
        }
        return self.send_message(message)
    
    def unsubscribe_market_data(self, symbol: str, exchange: str = 'CME'):
        """Unsubscribe from market data for a symbol"""
        message = {
            'action': 'UNSUBSCRIBE',
            'type': 'MARKET_DATA',
            'symbol': symbol,
            'exchange': exchange
        }
        return self.send_message(message)
    
    def set_callback(self, callback_type: str, callback: Callable):
        """Set callback function for specific data type"""
        self.callbacks[callback_type] = callback
    
    def disconnect(self):
        """Disconnect from R|Trader Pro"""
        self.running = False
        self.connected = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        logger.info("Disconnected from R|Trader Pro")


class RTraderProDataCollector:
    def __init__(self):
        self.running = True
        self.db_conn = None
        self.redis_client = None
        self.rtrader_client = None
        self.symbols = []
        
        # Database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'rithmic_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
        
        # Redis configuration
        self.redis_config = {
            'host': os.getenv('REDIS_HOST', 'localhost'),
            'port': int(os.getenv('REDIS_PORT', 6379))
        }
        
        # R|Trader Pro configuration
        self.rtrader_host = os.getenv('RTRADER_HOST', 'localhost')
        self.rtrader_port = int(os.getenv('RTRADER_PORT', 3012))
        
        # Load symbols from environment
        symbols_str = os.getenv('SYMBOLS', 'GCQ5,MGCQ5')
        self.symbols = [s.strip() for s in symbols_str.split(',')]
        
        # Track subscribed symbols
        self.subscribed_symbols = set()
    
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            self.db_conn.autocommit = False
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def connect_redis(self):
        """Connect to Redis for inter-service communication"""
        try:
            self.redis_client = redis.Redis(**self.redis_config, decode_responses=True)
            self.redis_client.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def connect_rtrader_pro(self):
        """Connect to R|Trader Pro Plugin API"""
        logger.info(f"Connecting to R|Trader Pro at {self.rtrader_host}:{self.rtrader_port}...")
        
        self.rtrader_client = RTraderProPlugin(
            host=self.rtrader_host,
            port=self.rtrader_port
        )
        
        if self.rtrader_client.connect():
            logger.info("Connected to R|Trader Pro successfully")
            
            # Setup callbacks
            self.rtrader_client.set_callback('tick_callback', self._handle_tick_data)
            self.rtrader_client.set_callback('quote_callback', self._handle_quote_data)
            self.rtrader_client.set_callback('level2_callback', self._handle_level2_data)
        else:
            raise Exception("Failed to connect to R|Trader Pro")
    
    def _handle_tick_data(self, data: Dict):
        """Handle incoming tick/trade data from R|Trader Pro"""
        try:
            cursor = self.db_conn.cursor()
            
            # Extract data fields
            timestamp = data.get('timestamp', datetime.now())
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            
            # Insert tick data
            insert_query = """
                INSERT INTO tick_data (
                    symbol, exchange, timestamp, price, size,
                    bid_price, ask_price, bid_size, ask_size
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                data.get('symbol'),
                data.get('exchange', 'CME'),
                timestamp,
                Decimal(str(data.get('price', 0))),
                data.get('size', 0),
                Decimal(str(data.get('bid_price', 0))) if data.get('bid_price') else None,
                Decimal(str(data.get('ask_price', 0))) if data.get('ask_price') else None,
                data.get('bid_size'),
                data.get('ask_size')
            ))
            
            self.db_conn.commit()
            logger.debug(f"Stored tick data for {data.get('symbol')}")
            
        except Exception as e:
            logger.error(f"Failed to store tick data: {e}")
            self.db_conn.rollback()
    
    def _handle_quote_data(self, data: Dict):
        """Handle incoming quote data from R|Trader Pro"""
        try:
            # Quote data can be stored as tick data with bid/ask prices
            self._handle_tick_data(data)
        except Exception as e:
            logger.error(f"Failed to handle quote data: {e}")
    
    def _handle_level2_data(self, data: Dict):
        """Handle incoming Level 2 data from R|Trader Pro"""
        try:
            cursor = self.db_conn.cursor()
            
            # Extract timestamp
            timestamp = data.get('timestamp', datetime.now())
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            
            # Clear old Level 2 data for this symbol
            delete_query = """
                DELETE FROM level2_data 
                WHERE symbol = %s AND timestamp < %s - INTERVAL '1 minute'
            """
            cursor.execute(delete_query, (data.get('symbol'), timestamp))
            
            # Insert new Level 2 data
            insert_query = """
                INSERT INTO level2_data (
                    symbol, exchange, timestamp, side, level, price, size
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            # Insert bid levels
            for level, bid in enumerate(data.get('bids', [])[:10]):  # Top 10 levels
                cursor.execute(insert_query, (
                    data.get('symbol'),
                    data.get('exchange', 'CME'),
                    timestamp,
                    'B',
                    level + 1,
                    Decimal(str(bid.get('price', 0))),
                    bid.get('size', 0)
                ))
            
            # Insert ask levels
            for level, ask in enumerate(data.get('asks', [])[:10]):  # Top 10 levels
                cursor.execute(insert_query, (
                    data.get('symbol'),
                    data.get('exchange', 'CME'),
                    timestamp,
                    'S',
                    level + 1,
                    Decimal(str(ask.get('price', 0))),
                    ask.get('size', 0)
                ))
            
            self.db_conn.commit()
            logger.debug(f"Stored Level 2 data for {data.get('symbol')}")
            
        except Exception as e:
            logger.error(f"Failed to store Level 2 data: {e}")
            self.db_conn.rollback()
    
    def subscribe_symbols(self):
        """Subscribe to market data for configured symbols"""
        logger.info(f"Subscribing to symbols: {self.symbols}")
        
        for symbol in self.symbols:
            # Parse symbol and exchange (format: SYMBOL or SYMBOL:EXCHANGE)
            if ':' in symbol:
                sym, exchange = symbol.split(':')
            else:
                sym = symbol
                exchange = 'CME'  # Default exchange
            
            if self.rtrader_client.subscribe_market_data(sym, exchange):
                self.subscribed_symbols.add(symbol)
                logger.info(f"Subscribed to {sym} on {exchange}")
            else:
                logger.error(f"Failed to subscribe to {sym}")
    
    def check_new_subscriptions(self):
        """Check Redis for new symbol subscription requests"""
        try:
            # Check for new subscription messages
            message = self.redis_client.lpop('symbol_subscriptions')
            if message:
                data = json.loads(message)
                symbol = data['symbol']
                exchange = data.get('exchange', 'CME')
                
                full_symbol = f"{symbol}:{exchange}"
                if full_symbol not in self.subscribed_symbols:
                    if self.rtrader_client.subscribe_market_data(symbol, exchange):
                        self.subscribed_symbols.add(full_symbol)
                        logger.info(f"Added new subscription: {symbol} on {exchange}")
        except Exception as e:
            logger.error(f"Failed to check new subscriptions: {e}")
    
    def run(self):
        """Main collector loop"""
        logger.info("Starting R|Trader Pro Data Collector...")
        
        try:
            # Connect to services
            self.connect_database()
            self.connect_redis()
            self.connect_rtrader_pro()
            self.subscribe_symbols()
            
            logger.info("Collector started successfully, waiting for data...")
            
            # Main collection loop
            while self.running:
                try:
                    # Check for new symbol subscriptions
                    self.check_new_subscriptions()
                    
                    # Sleep briefly to prevent CPU spinning
                    asyncio.get_event_loop().run_until_complete(asyncio.sleep(1))
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    asyncio.get_event_loop().run_until_complete(asyncio.sleep(1))
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            # Cleanup
            logger.info("Shutting down collector...")
            if self.rtrader_client:
                self.rtrader_client.disconnect()
            if self.db_conn:
                self.db_conn.close()
            if self.redis_client:
                self.redis_client.close()
            logger.info("Collector stopped")


if __name__ == "__main__":
    # Check if we should use R|Trader Pro mode
    use_rtrader = os.getenv('USE_RTRADER_PRO', 'false').lower() == 'true'
    
    if use_rtrader:
        logger.info("Starting in R|Trader Pro Plugin Mode")
        collector = RTraderProDataCollector()
        collector.run()
    else:
        logger.info("R|Trader Pro mode not enabled. Set USE_RTRADER_PRO=true to enable.")
        logger.info("To use async_rithmic mode, run collector.py instead.")

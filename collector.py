#!/usr/bin/env python3
"""
Rithmic Data Collector using async_rithmic
Collects real-time tick data and Level 2 market depth data
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import List, Dict, Any

import psycopg2
import redis
from async_rithmic import RithmicClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Debug: Print environment variables at startup
print(f"DEBUG: RITHMIC_SYSTEM_NAME = {os.getenv('RITHMIC_SYSTEM_NAME', 'NOT_SET')}")
print(f"DEBUG: RITHMIC_USER = {os.getenv('RITHMIC_USER', 'NOT_SET')}")
print(f"DEBUG: All RITHMIC env vars:")
for key, value in os.environ.items():
    if key.startswith('RITHMIC'):
        print(f"DEBUG: {key} = {value}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RithmicDataCollector:
    def __init__(self):
        # Rithmic credentials
        self.rithmic_user = os.getenv('RITHMIC_USER')
        self.rithmic_password = os.getenv('RITHMIC_PASSWORD')
        self.rithmic_system_name = os.getenv('RITHMIC_SYSTEM_NAME', 'Rithmic Test')
        self.rithmic_app_name = os.getenv('RITHMIC_APP_NAME', 'RithmicDataCollector')
        self.rithmic_app_version = os.getenv('RITHMIC_APP_VERSION', '1.0')
        self.rithmic_url = os.getenv('RITHMIC_URL', 'rituz00100.rithmic.com:443')
        
        # Debug logging for credentials
        logger.info(f"Loaded Rithmic credentials:")
        logger.info(f"  User: {self.rithmic_user}")
        logger.info(f"  System Name: {self.rithmic_system_name}")
        logger.info(f"  App Name: {self.rithmic_app_name}")
        logger.info(f"  App Version: {self.rithmic_app_version}")
        logger.info(f"  URL: {self.rithmic_url}")
        
        # Database configuration
        self.db_host = os.getenv('DB_HOST', 'db')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME', 'rithmic_db')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'securepassword')
        
        # Redis configuration
        self.redis_host = os.getenv('REDIS_HOST', 'redis')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        
        # Load initial symbols
        self.symbols = self._load_symbols()
        
        # Initialize connections
        self.rithmic_client = None
        self.db_conn = None
        self.redis_client = None
        
        # Track subscribed symbols
        self.subscribed_symbols = set()
        
    def _load_symbols(self) -> List[str]:
        """Load symbols from environment variable or config file"""
        # Try environment variable first
        symbols_env = os.getenv('SYMBOLS')
        if symbols_env:
            return [s.strip() for s in symbols_env.split(',') if s.strip()]
        
        # Try config.json file
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                return config.get('symbols', ['ESZ23'])
        except FileNotFoundError:
            logger.warning("No config.json found, using default symbol ESZ23")
            return ['ESZ23']
        except json.JSONDecodeError:
            logger.error("Invalid JSON in config.json, using default symbol ESZ23")
            return ['ESZ23']
    
    async def connect_rithmic(self):
        """Connect to Rithmic API"""
        try:
            self.rithmic_client = RithmicClient(
                user=self.rithmic_user,
                password=self.rithmic_password,
                system_name=self.rithmic_system_name,
                app_name=self.rithmic_app_name,
                app_version=self.rithmic_app_version,
                url=self.rithmic_url
            )
            
            # Register event handlers
            self.rithmic_client.on_connected += self._on_connected
            self.rithmic_client.on_disconnected += self._on_disconnected
            
            await self.rithmic_client.connect()
            logger.info("Connected to Rithmic successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to Rithmic: {e}")
            raise
    
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            self.db_conn.autocommit = True
            logger.info("Connected to PostgreSQL successfully")
            
            # Create tables if they don't exist
            self._create_tables()
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def connect_redis(self):
        """Connect to Redis for dynamic subscriptions"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Connected to Redis successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        cursor = self.db_conn.cursor()
        
        # Create tick_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tick_data (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                volume INTEGER NOT NULL,
                direction VARCHAR(10),
                trade_type VARCHAR(50),
                exchange VARCHAR(50)
            )
        """)
        
        # Create level2_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS level2_data (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                update_type VARCHAR(50),
                bids JSONB,
                asks JSONB,
                depth INTEGER
            )
        """)
        
        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tick_data_timestamp_symbol 
            ON tick_data(timestamp, symbol)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_level2_data_timestamp_symbol 
            ON level2_data(timestamp, symbol)
        """)
        
        cursor.close()
        logger.info("Database tables created/verified successfully")
    
    async def _on_connected(self, plant_type: str):
        """Handle connection events"""
        logger.info(f"Connected to plant: {plant_type}")
    
    async def _on_disconnected(self, plant_type: str):
        """Handle disconnection events"""
        logger.warning(f"Disconnected from plant: {plant_type}")
    
    async def _tick_data_handler(self, data: Dict[str, Any]):
        """Handle incoming tick data"""
        try:
            # Extract tick data fields
            timestamp = datetime.now().isoformat()
            symbol = data.get('symbol', '')
            price = float(data.get('last_trade_price', 0))
            volume = int(data.get('last_trade_size', 0))
            direction = 'buy' if data.get('aggressor_side') == 1 else 'sell'
            trade_type = data.get('trade_type', '')
            exchange = data.get('exchange', '')
            
            # Insert into database
            cursor = self.db_conn.cursor()
            cursor.execute("""
                INSERT INTO tick_data (timestamp, symbol, price, volume, direction, trade_type, exchange)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (timestamp, symbol, price, volume, direction, trade_type, exchange))
            cursor.close()
            
            logger.debug(f"Tick data saved: {symbol} @ {price} vol {volume}")
            
        except Exception as e:
            logger.error(f"Error handling tick data: {e}")
    
    async def _level2_data_handler(self, data: Dict[str, Any]):
        """Handle incoming Level 2 market depth data"""
        try:
            # Extract Level 2 data fields
            timestamp = datetime.now().isoformat()
            symbol = data.get('symbol', '')
            update_type = data.get('update_type', '')
            
            # Process bids and asks
            bids = []
            asks = []
            
            if 'bids' in data:
                for i, bid in enumerate(data['bids'][:10]):  # Top 10 levels
                    bids.append({
                        'level': i + 1,
                        'price': float(bid.get('price', 0)),
                        'size': int(bid.get('size', 0)),
                        'num_orders': int(bid.get('num_orders', 0))
                    })
            
            if 'asks' in data:
                for i, ask in enumerate(data['asks'][:10]):  # Top 10 levels
                    asks.append({
                        'level': i + 1,
                        'price': float(ask.get('price', 0)),
                        'size': int(ask.get('size', 0)),
                        'num_orders': int(ask.get('num_orders', 0))
                    })
            
            depth = len(bids) + len(asks)
            
            # Insert into database
            cursor = self.db_conn.cursor()
            cursor.execute("""
                INSERT INTO level2_data (timestamp, symbol, update_type, bids, asks, depth)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (timestamp, symbol, update_type, json.dumps(bids), json.dumps(asks), depth))
            cursor.close()
            
            logger.debug(f"Level 2 data saved: {symbol} depth {depth}")
            
        except Exception as e:
            logger.error(f"Error handling Level 2 data: {e}")
    
    async def subscribe_to_symbol(self, symbol: str, exchange: str = 'CME'):
        """Subscribe to tick data and market depth for a symbol"""
        try:
            if symbol in self.subscribed_symbols:
                logger.info(f"Already subscribed to {symbol}")
                return
            
            # Subscribe to tick data
            await self.rithmic_client.stream_market_data(
                symbol=symbol,
                exchange=exchange,
                callback=self._tick_data_handler
            )
            
            # Subscribe to Level 2 data (if supported)
            try:
                await self.rithmic_client.stream_level2_data(
                    symbol=symbol,
                    exchange=exchange,
                    callback=self._level2_data_handler
                )
            except AttributeError:
                logger.warning(f"Level 2 data not supported for {symbol}")
            
            self.subscribed_symbols.add(symbol)
            logger.info(f"Subscribed to {symbol} on {exchange}")
            
        except Exception as e:
            logger.error(f"Failed to subscribe to {symbol}: {e}")
    
    async def check_dynamic_subscriptions(self):
        """Check Redis for new symbol subscription requests"""
        try:
            # Check for new symbols in Redis queue
            new_symbol = self.redis_client.lpop('new_symbols')
            if new_symbol:
                symbol_data = json.loads(new_symbol)
                symbol = symbol_data.get('symbol')
                exchange = symbol_data.get('exchange', 'CME')
                
                logger.info(f"New subscription request: {symbol}")
                await self.subscribe_to_symbol(symbol, exchange)
                
        except Exception as e:
            logger.error(f"Error checking dynamic subscriptions: {e}")
    
    async def run(self):
        """Main collector loop"""
        try:
            # Connect to all services
            await self.connect_rithmic()
            self.connect_database()
            self.connect_redis()
            
            # Subscribe to initial symbols
            for symbol in self.symbols:
                await self.subscribe_to_symbol(symbol)
            
            logger.info(f"Data collector started for symbols: {self.symbols}")
            
            # Main loop
            while True:
                # Check for dynamic subscriptions every 5 seconds
                await self.check_dynamic_subscriptions()
                await asyncio.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("Collector stopped by user")
        except Exception as e:
            logger.error(f"Collector error: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Clean up connections"""
        try:
            if self.rithmic_client:
                await self.rithmic_client.disconnect()
            if self.db_conn:
                self.db_conn.close()
            if self.redis_client:
                self.redis_client.close()
            logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    collector = RithmicDataCollector()
    asyncio.run(collector.run())
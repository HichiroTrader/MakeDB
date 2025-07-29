#!/usr/bin/env python3
"""
Rithmic Binary Protocol Collector for R|Trader Pro
Uses Rithmic's proprietary binary protocol via pyrithmic library
"""

import os
import sys
import asyncio
import logging
import psycopg2
import redis
import json
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional
import struct

# Try to import pyrithmic or use custom implementation
try:
    import pyrithmic
    HAS_PYRITHMIC = True
except ImportError:
    HAS_PYRITHMIC = False
    logging.warning("pyrithmic not found, using custom binary protocol implementation")

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Rithmic message types (binary protocol)
RITHMIC_MSG_TYPES = {
    # Market Data Messages
    100: 'MARKET_DATA_REQUEST',
    101: 'MARKET_DATA_RESPONSE',
    102: 'LAST_TRADE',
    103: 'BID_OFFER',
    104: 'MARKET_MODE',
    105: 'OPEN_INTEREST',
    106: 'SETTLEMENT_PRICE',
    107: 'MARKET_DEPTH_REQUEST',
    108: 'MARKET_DEPTH_RESPONSE',
    109: 'MARKET_DEPTH_UPDATE',
    110: 'TRADE_VOLUME',
    111: 'OPEN_RANGE',
    112: 'HIGH_LOW',
    113: 'TRADE_STATISTICS',
    
    # History Messages
    200: 'HISTORY_REQUEST',
    201: 'HISTORY_RESPONSE',
    202: 'HISTORY_TICK',
    203: 'HISTORY_BAR',
    
    # PNL Messages
    300: 'PNL_REQUEST',
    301: 'PNL_RESPONSE',
    302: 'PNL_UPDATE',
}

# Symbol mapping for different exchanges
SYMBOL_EXCHANGE_MAP = {
    # Gold futures (COMEX)
    'GC': 'COMEX',
    'MGC': 'COMEX',
    'GLD': 'COMEX',
    
    # Energy (NYMEX)
    'CL': 'NYMEX',
    'NG': 'NYMEX',
    'RB': 'NYMEX',
    'HO': 'NYMEX',
    
    # Indices (CME)
    'ES': 'CME',
    'NQ': 'CME',
    'YM': 'CBOT',
    'RTY': 'CME',
    
    # Currencies (CME)
    '6A': 'CME',
    '6B': 'CME',
    '6C': 'CME',
    '6E': 'CME',
    '6J': 'CME',
    '6S': 'CME',
    
    # Agriculture (CBOT/CME)
    'ZC': 'CBOT',
    'ZS': 'CBOT',
    'ZW': 'CBOT',
    'ZL': 'CBOT',
    'ZM': 'CBOT',
    
    # Metals (COMEX)
    'SI': 'COMEX',
    'HG': 'COMEX',
    'PA': 'NYMEX',
    'PL': 'NYMEX',
}

class RithmicBinaryProtocol:
    """Handle Rithmic's binary protocol"""
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.connected = False
        
    async def connect(self):
        """Connect to Rithmic server"""
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            self.connected = True
            logger.info(f"Connected to Rithmic at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from server"""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        self.connected = False
        logger.info("Disconnected from Rithmic")
    
    async def send_message(self, msg_type: int, data: bytes):
        """Send binary message to Rithmic"""
        if not self.connected:
            return False
        
        try:
            # Rithmic binary format: [length(4 bytes)][type(2 bytes)][data]
            msg_length = len(data) + 2  # +2 for message type
            header = struct.pack('>IH', msg_length, msg_type)
            
            self.writer.write(header + data)
            await self.writer.drain()
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    async def receive_message(self):
        """Receive binary message from Rithmic"""
        if not self.connected:
            return None, None
        
        try:
            # Read message length (4 bytes)
            length_data = await self.reader.readexactly(4)
            msg_length = struct.unpack('>I', length_data)[0]
            
            # Read message type (2 bytes)
            type_data = await self.reader.readexactly(2)
            msg_type = struct.unpack('>H', type_data)[0]
            
            # Read message data
            data_length = msg_length - 2
            if data_length > 0:
                data = await self.reader.readexactly(data_length)
            else:
                data = b''
            
            return msg_type, data
        except asyncio.IncompleteReadError:
            logger.warning("Connection closed by server")
            self.connected = False
            return None, None
        except Exception as e:
            logger.error(f"Failed to receive message: {e}")
            return None, None
    
    def parse_market_data(self, msg_type: int, data: bytes) -> Dict:
        """Parse market data messages"""
        result = {'type': RITHMIC_MSG_TYPES.get(msg_type, 'UNKNOWN')}
        
        if msg_type == 102:  # LAST_TRADE
            # Parse binary data (this is simplified, actual format depends on Rithmic docs)
            if len(data) >= 24:
                symbol_len = struct.unpack('>I', data[0:4])[0]
                symbol = data[4:4+symbol_len].decode('utf-8')
                offset = 4 + symbol_len
                
                price = struct.unpack('>d', data[offset:offset+8])[0]
                size = struct.unpack('>I', data[offset+8:offset+12])[0]
                timestamp = struct.unpack('>Q', data[offset+12:offset+20])[0]
                
                result.update({
                    'symbol': symbol,
                    'price': price,
                    'size': size,
                    'timestamp': datetime.fromtimestamp(timestamp / 1000.0)
                })
        
        elif msg_type == 103:  # BID_OFFER
            # Parse bid/offer data
            if len(data) >= 32:
                symbol_len = struct.unpack('>I', data[0:4])[0]
                symbol = data[4:4+symbol_len].decode('utf-8')
                offset = 4 + symbol_len
                
                bid_price = struct.unpack('>d', data[offset:offset+8])[0]
                bid_size = struct.unpack('>I', data[offset+8:offset+12])[0]
                ask_price = struct.unpack('>d', data[offset+12:offset+20])[0]
                ask_size = struct.unpack('>I', data[offset+20:offset+24])[0]
                
                result.update({
                    'symbol': symbol,
                    'bid_price': bid_price,
                    'bid_size': bid_size,
                    'ask_price': ask_price,
                    'ask_size': ask_size
                })
        
        elif msg_type == 109:  # MARKET_DEPTH_UPDATE
            # Parse market depth
            # This would need proper binary parsing based on Rithmic's format
            pass
        
        return result

class RithmicCollector:
    def __init__(self):
        self.db_conn = None
        self.redis_client = None
        self.protocol = None
        self.running = True
        
        # Configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'rithmic_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
        
        # R|Trader Pro ports
        self.ticker_port = int(os.getenv('RTRADER_PORT', 3010))  # Market data
        self.history_port = int(os.getenv('RTRADER_HISTORY_PORT', 3013))  # History
        self.host = os.getenv('RTRADER_HOST', 'host.docker.internal')
        
        # Load symbols
        symbols_str = os.getenv('SYMBOLS', 'GCQ5,ESU5')
        self.symbols = [s.strip() for s in symbols_str.split(',')]
        
    def connect_database(self):
        """Connect to PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            self.db_conn.autocommit = False
            logger.info("Connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def connect_redis(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info("Connected to Redis")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False
    
    def get_exchange_for_symbol(self, symbol: str) -> str:
        """Get exchange for a symbol"""
        # Remove month/year code to get base symbol
        base = symbol.rstrip('0123456789')
        for prefix, exchange in SYMBOL_EXCHANGE_MAP.items():
            if base.startswith(prefix):
                return exchange
        return 'CME'  # Default
    
    async def subscribe_symbol(self, protocol: RithmicBinaryProtocol, symbol: str):
        """Subscribe to market data for a symbol"""
        exchange = self.get_exchange_for_symbol(symbol)
        logger.info(f"Subscribing to {symbol} on {exchange}")
        
        # Build subscription message (format depends on Rithmic protocol)
        # This is a simplified example
        data = struct.pack('>I', len(symbol)) + symbol.encode('utf-8')
        data += struct.pack('>I', len(exchange)) + exchange.encode('utf-8')
        data += struct.pack('>I', 1)  # Subscribe flag
        
        await protocol.send_message(100, data)  # MARKET_DATA_REQUEST
    
    def store_tick_data(self, data: Dict):
        """Store tick data in database"""
        if 'symbol' not in data or 'price' not in data:
            return
        
        try:
            cursor = self.db_conn.cursor()
            
            # Determine aggressor side if possible
            aggressor_side = None
            if 'aggressor' in data:
                aggressor_side = 'BUY' if data['aggressor'] == 1 else 'SELL'
            
            cursor.execute("""
                INSERT INTO tick_data (
                    timestamp, symbol, exchange, price, size,
                    bid_price, ask_price, bid_size, ask_size,
                    aggressor_side, trade_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data.get('timestamp', datetime.now()),
                data['symbol'],
                self.get_exchange_for_symbol(data['symbol']),
                Decimal(str(data['price'])),
                data.get('size', 0),
                Decimal(str(data.get('bid_price', 0))) if data.get('bid_price') else None,
                Decimal(str(data.get('ask_price', 0))) if data.get('ask_price') else None,
                data.get('bid_size'),
                data.get('ask_size'),
                aggressor_side,
                data.get('trade_id')
            ))
            
            self.db_conn.commit()
            logger.debug(f"Stored tick for {data['symbol']} @ {data['price']}")
            
        except Exception as e:
            logger.error(f"Failed to store tick data: {e}")
            self.db_conn.rollback()
    
    def store_depth_data(self, data: Dict):
        """Store market depth data"""
        if 'symbol' not in data:
            return
        
        try:
            cursor = self.db_conn.cursor()
            timestamp = data.get('timestamp', datetime.now())
            
            # Store bid levels
            for level, bid in enumerate(data.get('bids', []), 1):
                cursor.execute("""
                    INSERT INTO level2_data (
                        timestamp, symbol, exchange, side, level,
                        price, size, order_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp,
                    data['symbol'],
                    self.get_exchange_for_symbol(data['symbol']),
                    'B',
                    level,
                    Decimal(str(bid['price'])),
                    bid['size'],
                    bid.get('order_count', 1)
                ))
            
            # Store ask levels
            for level, ask in enumerate(data.get('asks', []), 1):
                cursor.execute("""
                    INSERT INTO level2_data (
                        timestamp, symbol, exchange, side, level,
                        price, size, order_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp,
                    data['symbol'],
                    self.get_exchange_for_symbol(data['symbol']),
                    'S',
                    level,
                    Decimal(str(ask['price'])),
                    ask['size'],
                    ask.get('order_count', 1)
                ))
            
            self.db_conn.commit()
            logger.debug(f"Stored depth data for {data['symbol']}")
            
        except Exception as e:
            logger.error(f"Failed to store depth data: {e}")
            self.db_conn.rollback()
    
    async def process_messages(self, protocol: RithmicBinaryProtocol):
        """Process incoming messages"""
        while self.running and protocol.connected:
            msg_type, data = await protocol.receive_message()
            
            if msg_type is None:
                break
            
            # Parse message based on type
            parsed_data = protocol.parse_market_data(msg_type, data)
            
            if msg_type == 102:  # LAST_TRADE
                self.store_tick_data(parsed_data)
            elif msg_type == 103:  # BID_OFFER
                # Update bid/ask in tick data
                self.store_tick_data(parsed_data)
            elif msg_type == 109:  # MARKET_DEPTH_UPDATE
                self.store_depth_data(parsed_data)
            else:
                logger.debug(f"Received message type {msg_type}: {parsed_data}")
    
    async def check_new_subscriptions(self):
        """Check for new symbol subscriptions from Redis"""
        try:
            message = self.redis_client.lpop('symbol_subscriptions')
            if message:
                data = json.loads(message)
                symbol = data['symbol']
                if symbol not in self.symbols:
                    self.symbols.append(symbol)
                    logger.info(f"Adding new symbol: {symbol}")
                    
                    # Subscribe if connected
                    if self.protocol and self.protocol.connected:
                        await self.subscribe_symbol(self.protocol, symbol)
        except Exception as e:
            logger.error(f"Failed to check subscriptions: {e}")
    
    async def run(self):
        """Main collector loop"""
        logger.info("Starting Rithmic Binary Protocol Collector...")
        
        # Connect to databases
        if not self.connect_database():
            return
        if not self.connect_redis():
            return
        
        # Connect to R|Trader Pro (Ticker Plant)
        self.protocol = RithmicBinaryProtocol(self.host, self.ticker_port)
        
        retry_count = 0
        while retry_count < 5:
            if await self.protocol.connect():
                break
            retry_count += 1
            logger.warning(f"Connection attempt {retry_count} failed, retrying in 5 seconds...")
            await asyncio.sleep(5)
        
        if not self.protocol.connected:
            logger.error("Failed to connect to R|Trader Pro after 5 attempts")
            return
        
        # Subscribe to initial symbols
        for symbol in self.symbols:
            await self.subscribe_symbol(self.protocol, symbol)
        
        logger.info(f"Subscribed to {len(self.symbols)} symbols")
        
        # Create tasks
        tasks = [
            asyncio.create_task(self.process_messages(self.protocol)),
            asyncio.create_task(self.subscription_checker())
        ]
        
        try:
            # Run until interrupted
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.running = False
            await self.protocol.disconnect()
            
            if self.db_conn:
                self.db_conn.close()
            if self.redis_client:
                self.redis_client.close()
            
            logger.info("Collector stopped")
    
    async def subscription_checker(self):
        """Periodically check for new subscriptions"""
        while self.running:
            await self.check_new_subscriptions()
            await asyncio.sleep(5)


if __name__ == "__main__":
    # Check if we should use this collector
    use_rtrader = os.getenv('USE_RTRADER_PRO', 'false').lower() == 'true'
    
    if use_rtrader:
        logger.info("Starting Rithmic Binary Protocol Collector")
        collector = RithmicCollector()
        asyncio.run(collector.run())
    else:
        logger.info("R|Trader Pro mode not enabled")

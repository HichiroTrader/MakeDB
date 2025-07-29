#!/usr/bin/env python3
"""
Flask API for RithmicDataCollector
Provides endpoints to retrieve tick data and manage symbol subscriptions
"""

import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any

import psycopg2
import redis
from flask import Flask, jsonify, request
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class DatabaseManager:
    def __init__(self):
        self.db_host = os.getenv('DB_HOST', 'db')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME', 'rithmic_db')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'securepassword')
        
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password
        )

class RedisManager:
    def __init__(self):
        self.redis_host = os.getenv('REDIS_HOST', 'redis')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        
    def get_client(self):
        """Get Redis client"""
        return redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True
        )

# Initialize managers
db_manager = DatabaseManager()
redis_manager = RedisManager()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.close()
        conn.close()
        
        # Test Redis connection
        redis_client = redis_manager.get_client()
        redis_client.ping()
        redis_client.close()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'services': {
                'database': 'connected',
                'redis': 'connected'
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 500

@app.route('/api/ticks/<symbol>', methods=['GET'])
def get_tick_data(symbol: str):
    """Get recent tick data for a symbol"""
    try:
        # Get limit parameter (default 100)
        limit = request.args.get('limit', 100, type=int)
        if limit > 1000:  # Prevent excessive data requests
            limit = 1000
        
        # Get tick data from database
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT timestamp, symbol, price, volume, direction, trade_type, exchange
            FROM tick_data
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (symbol.upper(), limit))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Format response
        tick_data = []
        for row in rows:
            tick_data.append({
                'timestamp': row[0].isoformat() if row[0] else None,
                'symbol': row[1],
                'price': float(row[2]) if row[2] is not None else None,
                'volume': int(row[3]) if row[3] is not None else None,
                'direction': row[4],
                'trade_type': row[5],
                'exchange': row[6]
            })
        
        return jsonify({
            'symbol': symbol.upper(),
            'count': len(tick_data),
            'limit': limit,
            'data': tick_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving tick data for {symbol}: {e}")
        return jsonify({
            'error': 'Failed to retrieve tick data',
            'message': str(e)
        }), 500

@app.route('/api/level2/<symbol>', methods=['GET'])
def get_level2_data(symbol: str):
    """Get recent Level 2 market depth data for a symbol"""
    try:
        # Get limit parameter (default 50)
        limit = request.args.get('limit', 50, type=int)
        if limit > 500:  # Prevent excessive data requests
            limit = 500
        
        # Get Level 2 data from database
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT timestamp, symbol, update_type, bids, asks, depth
            FROM level2_data
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (symbol.upper(), limit))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Format response
        level2_data = []
        for row in rows:
            level2_data.append({
                'timestamp': row[0].isoformat() if row[0] else None,
                'symbol': row[1],
                'update_type': row[2],
                'bids': json.loads(row[3]) if row[3] else [],
                'asks': json.loads(row[4]) if row[4] else [],
                'depth': int(row[5]) if row[5] is not None else 0
            })
        
        return jsonify({
            'symbol': symbol.upper(),
            'count': len(level2_data),
            'limit': limit,
            'data': level2_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving Level 2 data for {symbol}: {e}")
        return jsonify({
            'error': 'Failed to retrieve Level 2 data',
            'message': str(e)
        }), 500

@app.route('/api/subscribe/<symbol>', methods=['POST'])
def subscribe_symbol(symbol: str):
    """Add a new symbol to the collector's subscription list"""
    try:
        # Get exchange from request body (optional)
        data = request.get_json() or {}
        exchange = data.get('exchange', 'CME')
        
        # Add symbol to Redis queue for collector to pick up
        redis_client = redis_manager.get_client()
        
        subscription_request = {
            'symbol': symbol.upper(),
            'exchange': exchange.upper(),
            'timestamp': datetime.now().isoformat()
        }
        
        redis_client.rpush('new_symbols', json.dumps(subscription_request))
        redis_client.close()
        
        logger.info(f"Subscription request added for {symbol} on {exchange}")
        
        return jsonify({
            'status': 'subscribed',
            'symbol': symbol.upper(),
            'exchange': exchange.upper(),
            'message': f'Subscription request for {symbol} has been queued'
        }), 200
        
    except Exception as e:
        logger.error(f"Error subscribing to {symbol}: {e}")
        return jsonify({
            'error': 'Failed to subscribe to symbol',
            'message': str(e)
        }), 500

@app.route('/api/symbols', methods=['GET'])
def get_symbols():
    """Get list of symbols with recent data"""
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Get symbols with tick data in the last 24 hours
        cursor.execute("""
            SELECT symbol, exchange, COUNT(*) as tick_count, 
                   MAX(timestamp) as last_update
            FROM tick_data
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY symbol, exchange
            ORDER BY last_update DESC
        """)
        
        tick_symbols = cursor.fetchall()
        
        # Get symbols with Level 2 data in the last 24 hours
        cursor.execute("""
            SELECT symbol, COUNT(*) as level2_count,
                   MAX(timestamp) as last_update
            FROM level2_data
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY symbol
            ORDER BY last_update DESC
        """)
        
        level2_symbols = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Format response
        symbols_data = {}
        
        # Add tick data symbols
        for row in tick_symbols:
            symbol = row[0]
            symbols_data[symbol] = {
                'symbol': symbol,
                'exchange': row[1],
                'tick_count': int(row[2]),
                'last_tick_update': row[3].isoformat() if row[3] else None,
                'level2_count': 0,
                'last_level2_update': None
            }
        
        # Add Level 2 data info
        for row in level2_symbols:
            symbol = row[0]
            if symbol in symbols_data:
                symbols_data[symbol]['level2_count'] = int(row[1])
                symbols_data[symbol]['last_level2_update'] = row[2].isoformat() if row[2] else None
            else:
                symbols_data[symbol] = {
                    'symbol': symbol,
                    'exchange': 'Unknown',
                    'tick_count': 0,
                    'last_tick_update': None,
                    'level2_count': int(row[1]),
                    'last_level2_update': row[2].isoformat() if row[2] else None
                }
        
        return jsonify({
            'count': len(symbols_data),
            'symbols': list(symbols_data.values())
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving symbols: {e}")
        return jsonify({
            'error': 'Failed to retrieve symbols',
            'message': str(e)
        }), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Get tick data stats
        cursor.execute("""
            SELECT COUNT(*) as total_ticks,
                   COUNT(DISTINCT symbol) as unique_symbols,
                   MIN(timestamp) as earliest_tick,
                   MAX(timestamp) as latest_tick
            FROM tick_data
        """)
        
        tick_stats = cursor.fetchone()
        
        # Get Level 2 data stats
        cursor.execute("""
            SELECT COUNT(*) as total_level2,
                   MIN(timestamp) as earliest_level2,
                   MAX(timestamp) as latest_level2
            FROM level2_data
        """)
        
        level2_stats = cursor.fetchone()
        
        # Get recent activity (last hour)
        cursor.execute("""
            SELECT COUNT(*) as recent_ticks
            FROM tick_data
            WHERE timestamp > NOW() - INTERVAL '1 hour'
        """)
        
        recent_ticks = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) as recent_level2
            FROM level2_data
            WHERE timestamp > NOW() - INTERVAL '1 hour'
        """)
        
        recent_level2 = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'tick_data': {
                'total_records': int(tick_stats[0]) if tick_stats[0] else 0,
                'unique_symbols': int(tick_stats[1]) if tick_stats[1] else 0,
                'earliest_timestamp': tick_stats[2].isoformat() if tick_stats[2] else None,
                'latest_timestamp': tick_stats[3].isoformat() if tick_stats[3] else None,
                'recent_hour_count': int(recent_ticks)
            },
            'level2_data': {
                'total_records': int(level2_stats[0]) if level2_stats[0] else 0,
                'earliest_timestamp': level2_stats[1].isoformat() if level2_stats[1] else None,
                'latest_timestamp': level2_stats[2].isoformat() if level2_stats[2] else None,
                'recent_hour_count': int(recent_level2)
            },
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving stats: {e}")
        return jsonify({
            'error': 'Failed to retrieve statistics',
            'message': str(e)
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'error': 'Endpoint not found',
        'message': 'The requested endpoint does not exist'
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
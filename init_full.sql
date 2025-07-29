-- Enable TimescaleDB extension (optional but recommended for time-series data)
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop existing tables if needed (comment out in production)
-- DROP TABLE IF EXISTS tick_data CASCADE;
-- DROP TABLE IF EXISTS level2_data CASCADE;
-- DROP TABLE IF EXISTS symbols CASCADE;

-- Create symbols table
CREATE TABLE IF NOT EXISTS symbols (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL UNIQUE,
    exchange VARCHAR(50) DEFAULT 'CME',
    description TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create comprehensive tick data table (includes all trade information)
CREATE TABLE IF NOT EXISTS tick_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(50),
    
    -- Trade data
    price DECIMAL(20,8) NOT NULL,
    size INTEGER NOT NULL,
    trade_id VARCHAR(100),
    
    -- Best bid/ask at time of trade (MBP - Market By Price)
    bid_price DECIMAL(20,8),
    ask_price DECIMAL(20,8),
    bid_size INTEGER,
    ask_size INTEGER,
    
    -- Trade direction/aggressor (để phân biệt mua/bán chủ động)
    aggressor_side VARCHAR(10), -- 'BUY' or 'SELL'
    is_implied BOOLEAN DEFAULT FALSE,
    
    -- Additional fields
    trade_type VARCHAR(50),
    condition_codes VARCHAR(100),
    
    -- Volume delta tracking
    buy_volume INTEGER DEFAULT 0,
    sell_volume INTEGER DEFAULT 0,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create comprehensive Level 2 / Market Depth table
CREATE TABLE IF NOT EXISTS level2_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(50),
    
    -- Level information
    side CHAR(1) CHECK (side IN ('B', 'S')), -- 'B' for Bid, 'S' for Ask/Sell
    level INTEGER NOT NULL,
    
    -- Price and size at this level
    price DECIMAL(20,8) NOT NULL,
    size INTEGER NOT NULL,
    
    -- Number of orders at this level (Market by Price)
    order_count INTEGER,
    
    -- For implied orders
    implied_size INTEGER DEFAULT 0,
    
    -- Update type
    update_type VARCHAR(20), -- 'SNAPSHOT', 'ADD', 'DELETE', 'MODIFY'
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create order book updates table (for tracking individual order changes)
CREATE TABLE IF NOT EXISTS order_updates (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(50),
    
    -- Order information
    order_id VARCHAR(100),
    side CHAR(1) CHECK (side IN ('B', 'S')),
    price DECIMAL(20,8),
    size INTEGER,
    
    -- Update action
    action VARCHAR(20), -- 'ADD', 'MODIFY', 'DELETE', 'FILL'
    
    -- Fill information (if action = 'FILL')
    fill_size INTEGER,
    remaining_size INTEGER,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create table for tracking order flow / volume profile
CREATE TABLE IF NOT EXISTS volume_profile (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    price_level DECIMAL(20,8) NOT NULL,
    
    -- Volume at price level
    buy_volume INTEGER DEFAULT 0,
    sell_volume INTEGER DEFAULT 0,
    total_volume INTEGER DEFAULT 0,
    
    -- Number of trades
    buy_trades INTEGER DEFAULT 0,
    sell_trades INTEGER DEFAULT 0,
    
    -- Delta (buy - sell)
    delta INTEGER GENERATED ALWAYS AS (buy_volume - sell_volume) STORED,
    
    period_start TIMESTAMP WITH TIME ZONE,
    period_end TIMESTAMP WITH TIME ZONE
);

-- Create table for aggregated market statistics
CREATE TABLE IF NOT EXISTS market_stats (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    
    -- OHLCV data
    open_price DECIMAL(20,8),
    high_price DECIMAL(20,8),
    low_price DECIMAL(20,8),
    close_price DECIMAL(20,8),
    volume INTEGER,
    
    -- Bid/Ask statistics
    avg_bid_ask_spread DECIMAL(20,8),
    
    -- Order flow statistics
    buy_volume INTEGER,
    sell_volume INTEGER,
    delta INTEGER,
    
    -- Depth statistics
    total_bid_depth INTEGER,
    total_ask_depth INTEGER,
    
    period_minutes INTEGER DEFAULT 1
);

-- Create comprehensive indexes for performance
CREATE INDEX idx_tick_data_symbol_timestamp ON tick_data(symbol, timestamp DESC);
CREATE INDEX idx_tick_data_timestamp ON tick_data(timestamp DESC);
CREATE INDEX idx_tick_data_aggressor ON tick_data(symbol, aggressor_side, timestamp DESC);

CREATE INDEX idx_level2_data_symbol_timestamp ON level2_data(symbol, timestamp DESC);
CREATE INDEX idx_level2_data_symbol_side_level ON level2_data(symbol, side, level);
CREATE INDEX idx_level2_data_timestamp ON level2_data(timestamp DESC);

CREATE INDEX idx_order_updates_symbol_timestamp ON order_updates(symbol, timestamp DESC);
CREATE INDEX idx_order_updates_order_id ON order_updates(order_id);

CREATE INDEX idx_volume_profile_symbol_price ON volume_profile(symbol, price_level);
CREATE INDEX idx_volume_profile_symbol_timestamp ON volume_profile(symbol, timestamp DESC);

-- Insert initial symbols
INSERT INTO symbols (symbol, exchange, description) VALUES
    ('GCQ5', 'CME', 'Gold Futures Aug 2025'),
    ('MGCQ5', 'CME', 'Micro Gold Futures Aug 2025'),
    ('GCZ5', 'CME', 'Gold Futures Dec 2025'),
    ('MGCZ5', 'CME', 'Micro Gold Futures Dec 2025'),
    ('ESU5', 'CME', 'E-mini S&P 500 Sep 2025'),
    ('NQU5', 'CME', 'E-mini Nasdaq 100 Sep 2025'),
    ('CLU5', 'NYMEX', 'Crude Oil Sep 2025')
ON CONFLICT (symbol) DO NOTHING;

-- Create function to update timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for symbols table
CREATE TRIGGER update_symbols_updated_at BEFORE UPDATE ON symbols
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create view for easy access to latest market depth
CREATE OR REPLACE VIEW latest_market_depth AS
SELECT DISTINCT ON (symbol, side, level)
    symbol, side, level, price, size, order_count, timestamp
FROM level2_data
ORDER BY symbol, side, level, timestamp DESC;

-- Create view for volume profile summary
CREATE OR REPLACE VIEW volume_profile_summary AS
SELECT 
    symbol,
    price_level,
    SUM(buy_volume) as total_buy_volume,
    SUM(sell_volume) as total_sell_volume,
    SUM(total_volume) as total_volume,
    SUM(buy_volume) - SUM(sell_volume) as total_delta
FROM volume_profile
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1 day'
GROUP BY symbol, price_level
ORDER BY symbol, price_level;

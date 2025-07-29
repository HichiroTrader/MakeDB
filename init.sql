-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create tables
CREATE TABLE IF NOT EXISTS market_depth (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    bid_price DECIMAL(20,8),
    bid_size BIGINT,
    ask_price DECIMAL(20,8),
    ask_size BIGINT,
    level INTEGER NOT NULL,
    update_id BIGINT
);

CREATE TABLE IF NOT EXISTS trades (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    price DECIMAL(20,8) NOT NULL,
    size BIGINT NOT NULL,
    side VARCHAR(10),
    trade_id VARCHAR(100)
);

-- Convert to hypertables
SELECT create_hypertable('market_depth', 'time', if_not_exists => TRUE);
SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_market_depth_symbol_time ON market_depth (symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, time DESC);

-- Add compression policy (optional)
SELECT add_compression_policy('market_depth', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('trades', INTERVAL '7 days', if_not_exists => TRUE);
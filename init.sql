-- Initialize RithmicDataCollector Database
-- This script creates the necessary tables and indexes for storing tick data and Level 2 market depth

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create tick_data table for storing real-time trade data
CREATE TABLE IF NOT EXISTS tick_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    price DOUBLE PRECISION,
    volume INTEGER,
    direction VARCHAR(10),
    trade_type VARCHAR(50),
    exchange VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create level2_data table for storing market depth data
CREATE TABLE IF NOT EXISTS level2_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    update_type VARCHAR(50),
    bids JSONB,
    asks JSONB,
    depth INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better query performance

-- Indexes for tick_data table
CREATE INDEX IF NOT EXISTS idx_tick_data_timestamp ON tick_data(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tick_data_symbol ON tick_data(symbol);
CREATE INDEX IF NOT EXISTS idx_tick_data_symbol_timestamp ON tick_data(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tick_data_exchange ON tick_data(exchange);
CREATE INDEX IF NOT EXISTS idx_tick_data_created_at ON tick_data(created_at DESC);

-- Indexes for level2_data table
CREATE INDEX IF NOT EXISTS idx_level2_data_timestamp ON level2_data(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_level2_data_symbol ON level2_data(symbol);
CREATE INDEX IF NOT EXISTS idx_level2_data_symbol_timestamp ON level2_data(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_level2_data_update_type ON level2_data(update_type);
CREATE INDEX IF NOT EXISTS idx_level2_data_created_at ON level2_data(created_at DESC);

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_tick_data_symbol_exchange_timestamp ON tick_data(symbol, exchange, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_level2_data_symbol_update_type_timestamp ON level2_data(symbol, update_type, timestamp DESC);

-- Partial indexes with NOW() are not allowed in PostgreSQL
-- Using regular indexes instead for better performance

-- Create a table for storing symbol metadata
CREATE TABLE IF NOT EXISTS symbol_metadata (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) UNIQUE NOT NULL,
    exchange VARCHAR(50),
    description TEXT,
    tick_size DOUBLE PRECISION,
    contract_size INTEGER,
    currency VARCHAR(10),
    sector VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for symbol_metadata
CREATE INDEX IF NOT EXISTS idx_symbol_metadata_symbol ON symbol_metadata(symbol);
CREATE INDEX IF NOT EXISTS idx_symbol_metadata_exchange ON symbol_metadata(exchange);
CREATE INDEX IF NOT EXISTS idx_symbol_metadata_active ON symbol_metadata(is_active);

-- Create a table for tracking data collection statistics
CREATE TABLE IF NOT EXISTS collection_stats (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    tick_count INTEGER DEFAULT 0,
    level2_count INTEGER DEFAULT 0,
    first_tick_time TIMESTAMP WITH TIME ZONE,
    last_tick_time TIMESTAMP WITH TIME ZONE,
    first_level2_time TIMESTAMP WITH TIME ZONE,
    last_level2_time TIMESTAMP WITH TIME ZONE,
    avg_tick_volume DOUBLE PRECISION,
    max_tick_volume INTEGER,
    min_tick_volume INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, date)
);

-- Indexes for collection_stats
CREATE INDEX IF NOT EXISTS idx_collection_stats_symbol_date ON collection_stats(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_collection_stats_date ON collection_stats(date DESC);

-- Create a function to update symbol metadata when new data arrives
CREATE OR REPLACE FUNCTION update_symbol_metadata()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO symbol_metadata (symbol, exchange, last_seen, updated_at)
    VALUES (NEW.symbol, COALESCE(NEW.exchange, 'UNKNOWN'), NEW.timestamp, NOW())
    ON CONFLICT (symbol) DO UPDATE SET
        last_seen = NEW.timestamp,
        updated_at = NOW(),
        exchange = COALESCE(EXCLUDED.exchange, symbol_metadata.exchange);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers to automatically update symbol metadata
CREATE TRIGGER trigger_update_symbol_metadata_tick
    AFTER INSERT ON tick_data
    FOR EACH ROW
    EXECUTE FUNCTION update_symbol_metadata();

CREATE TRIGGER trigger_update_symbol_metadata_level2
    AFTER INSERT ON level2_data
    FOR EACH ROW
    EXECUTE FUNCTION update_symbol_metadata();

-- Create a function to update collection statistics
CREATE OR REPLACE FUNCTION update_collection_stats()
RETURNS TRIGGER AS $$
DECLARE
    stat_date DATE;
BEGIN
    stat_date := DATE(NEW.timestamp);
    
    IF TG_TABLE_NAME = 'tick_data' THEN
        INSERT INTO collection_stats (symbol, date, tick_count, first_tick_time, last_tick_time, 
                                    avg_tick_volume, max_tick_volume, min_tick_volume, updated_at)
        VALUES (NEW.symbol, stat_date, 1, NEW.timestamp, NEW.timestamp, 
                NEW.volume, NEW.volume, NEW.volume, NOW())
        ON CONFLICT (symbol, date) DO UPDATE SET
            tick_count = collection_stats.tick_count + 1,
            last_tick_time = NEW.timestamp,
            avg_tick_volume = (collection_stats.avg_tick_volume * collection_stats.tick_count + NEW.volume) / (collection_stats.tick_count + 1),
            max_tick_volume = GREATEST(collection_stats.max_tick_volume, NEW.volume),
            min_tick_volume = LEAST(collection_stats.min_tick_volume, NEW.volume),
            updated_at = NOW();
    ELSIF TG_TABLE_NAME = 'level2_data' THEN
        INSERT INTO collection_stats (symbol, date, level2_count, first_level2_time, last_level2_time, updated_at)
        VALUES (NEW.symbol, stat_date, 1, NEW.timestamp, NEW.timestamp, NOW())
        ON CONFLICT (symbol, date) DO UPDATE SET
            level2_count = collection_stats.level2_count + 1,
            last_level2_time = NEW.timestamp,
            updated_at = NOW();
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for collection statistics
CREATE TRIGGER trigger_update_collection_stats_tick
    AFTER INSERT ON tick_data
    FOR EACH ROW
    EXECUTE FUNCTION update_collection_stats();

CREATE TRIGGER trigger_update_collection_stats_level2
    AFTER INSERT ON level2_data
    FOR EACH ROW
    EXECUTE FUNCTION update_collection_stats();

-- Create a view for recent tick data (last 24 hours)
CREATE OR REPLACE VIEW recent_tick_data AS
SELECT *
FROM tick_data
WHERE timestamp > NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;

-- Create a view for recent level2 data (last 24 hours)
CREATE OR REPLACE VIEW recent_level2_data AS
SELECT *
FROM level2_data
WHERE timestamp > NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;

-- Create a view for symbol summary statistics
CREATE OR REPLACE VIEW symbol_summary AS
SELECT 
    sm.symbol,
    sm.exchange,
    sm.description,
    sm.is_active,
    sm.first_seen,
    sm.last_seen,
    COALESCE(SUM(cs.tick_count), 0) as total_ticks,
    COALESCE(SUM(cs.level2_count), 0) as total_level2_updates,
    MAX(cs.date) as last_data_date,
    COALESCE(AVG(cs.avg_tick_volume), 0) as avg_daily_tick_volume
FROM symbol_metadata sm
LEFT JOIN collection_stats cs ON sm.symbol = cs.symbol
GROUP BY sm.symbol, sm.exchange, sm.description, sm.is_active, sm.first_seen, sm.last_seen
ORDER BY sm.last_seen DESC;

-- Insert some example symbol metadata
INSERT INTO symbol_metadata (symbol, exchange, description, tick_size, contract_size, currency, sector) VALUES
('ESZ23', 'CME', 'E-mini S&P 500 December 2023', 0.25, 50, 'USD', 'Index'),
('CLZ23', 'NYMEX', 'Crude Oil December 2023', 0.01, 1000, 'USD', 'Energy'),
('NQZ23', 'CME', 'E-mini NASDAQ-100 December 2023', 0.25, 20, 'USD', 'Index'),
('YMZ23', 'CBOT', 'Mini Dow Jones December 2023', 1.0, 5, 'USD', 'Index'),
('RTY23', 'CME', 'E-mini Russell 2000 December 2023', 0.1, 50, 'USD', 'Index')
ON CONFLICT (symbol) DO NOTHING;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;

-- Create indexes for JSONB columns in level2_data for better performance
CREATE INDEX IF NOT EXISTS idx_level2_data_bids_gin ON level2_data USING GIN (bids);
CREATE INDEX IF NOT EXISTS idx_level2_data_asks_gin ON level2_data USING GIN (asks);

-- Analyze tables for better query planning
ANALYZE tick_data;
ANALYZE level2_data;
ANALYZE symbol_metadata;
ANALYZE collection_stats;

-- Print completion message
DO $$
BEGIN
    RAISE NOTICE 'RithmicDataCollector database initialization completed successfully!';
    RAISE NOTICE 'Created tables: tick_data, level2_data, symbol_metadata, collection_stats';
    RAISE NOTICE 'Created views: recent_tick_data, recent_level2_data, symbol_summary';
    RAISE NOTICE 'Created indexes and triggers for optimal performance';
END $$;
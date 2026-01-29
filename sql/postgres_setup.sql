-- Events table (main data store)
CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(100),
    event_type VARCHAR(20) NOT NULL,  -- 'view', 'purchase', 'cart_add'
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    price DECIMAL(10, 2),
    quantity INTEGER DEFAULT 1,
    timestamp TIMESTAMP NOT NULL,
    user_agent VARCHAR(255),
    ip_address INET,
    country VARCHAR(50),
    city VARCHAR(100),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_timestamp TIMESTAMP,
    CONSTRAINT valid_event_type CHECK (event_type IN ('view', 'purchase', 'cart_add', 'search'))
);


-- Indexes for query performance
CREATE INDEX idx_events_timestamp ON ecommerce_events(timestamp DESC);
CREATE INDEX idx_events_user_id ON ecommerce_events(user_id);
CREATE INDEX idx_events_product_id ON ecommerce_events(product_id);
CREATE INDEX idx_events_event_type ON ecommerce_events(event_type);
CREATE INDEX idx_events_ingestion ON ecommerce_events(ingestion_timestamp DESC);



-- Partitioning by date (for scalability)
CREATE TABLE ecommerce_events_2025_01 PARTITION OF ecommerce_events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');



-- Performance metrics table
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC,
    metric_unit VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);



-- Data quality tracking
CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(100),
    check_status VARCHAR(20),  -- 'passed', 'failed', 'warning'
    records_checked INTEGER,
    records_failed INTEGER,
    error_details TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
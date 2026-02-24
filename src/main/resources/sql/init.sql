-- Create tables for order processing
CREATE TABLE IF NOT EXISTS orders_to_publish (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    customer_id VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'PENDING',
    payment_method VARCHAR(20),
    description TEXT,
    published_status VARCHAR(20) DEFAULT 'PENDING',
    partition_number INTEGER,
    offset_number BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS orders_consumed (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'PENDING',
    payment_method VARCHAR(20),
    description TEXT,
    consumed_status VARCHAR(20) DEFAULT 'PENDING',
    consumed_from_partition INTEGER,
    consumed_from_offset BIGINT,
    consumed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    error_message TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_orders_to_publish_status ON orders_to_publish(published_status);
CREATE INDEX IF NOT EXISTS idx_orders_to_publish_order_id ON orders_to_publish(order_id);
CREATE INDEX IF NOT EXISTS idx_orders_consumed_status ON orders_consumed(consumed_status);
CREATE INDEX IF NOT EXISTS idx_orders_consumed_order_id ON orders_consumed(order_id);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update updated_at
CREATE TRIGGER update_orders_to_publish_updated_at 
    BEFORE UPDATE ON orders_to_publish 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert sample data only if table is empty
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM orders_to_publish) = 0 THEN
        -- Insert 2000 sample orders
        INSERT INTO orders_to_publish (order_id, customer_id, amount, currency, status, payment_method, description)
        SELECT 
            'ORD-' || LPAD(generate_series::text, 6, '0'),
            'CUST-' || LPAD((floor(random() * 1000) + 1)::text, 4, '0'),
            round((random() * 1000 + 10)::numeric, 2),
            CASE WHEN random() > 0.8 THEN 'EUR' WHEN random() > 0.6 THEN 'GBP' ELSE 'USD' END,
            CASE WHEN random() > 0.8 THEN 'FAILED' WHEN random() > 0.2 THEN 'COMPLETED' ELSE 'PENDING' END,
            CASE 
                WHEN random() > 0.7 THEN 'CREDIT_CARD'
                WHEN random() > 0.4 THEN 'DEBIT_CARD'
                WHEN random() > 0.2 THEN 'PAYPAL'
                ELSE 'BANK_TRANSFER'
            END,
            'Sample order ' || generate_series || ' for testing batch publishing'
        FROM generate_series(1, 2000);
        
        RAISE NOTICE 'Inserted 2000 sample orders into orders_to_publish table';
    ELSE
        RAISE NOTICE 'orders_to_publish table already contains data, skipping sample data insertion';
    END IF;
END $$;

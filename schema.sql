-- Schema for Transactional Outbox Pattern (Orders & Payments Learning Project)

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Orders table
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number VARCHAR(64) UNIQUE NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    customer_name VARCHAR(128) NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(32) NOT NULL DEFAULT 'CREATED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Order items table
CREATE TABLE IF NOT EXISTS order_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    subtotal NUMERIC(12, 2) NOT NULL CHECK (subtotal >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Outbox table
-- Captures events to be reliably published to message brokers (e.g., Kafka).
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(64) NOT NULL,          -- e.g. 'ORDER'
    aggregate_id VARCHAR(64) NOT NULL,            -- e.g. order_id
    event_type VARCHAR(64) NOT NULL,              -- e.g. 'OrderCreated'
    payload JSONB NOT NULL,                       -- Event payload consumed by payment service
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',-- 'PENDING', 'PROCESSED', 'FAILED'
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ NULL,
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT NULL
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_outbox_status_created_at ON outbox(status, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate ON outbox(aggregate_type, aggregate_id);

-- 4. Outbox Trigger Function
-- Automatically creates an outbox event whenever an order is inserted.
-- Ensures zero event omission at the database level.
CREATE OR REPLACE FUNCTION trigger_order_created_outbox()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO outbox (
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
        status,
        created_at
    ) VALUES (
        'ORDER',
        NEW.id::TEXT,
        'OrderCreated',
        jsonb_build_object(
            'event_id', gen_random_uuid(),
            'event_type', 'OrderCreated',
            'timestamp', NEW.created_at,
            'order', jsonb_build_object(
                'order_id', NEW.id,
                'order_number', NEW.order_number,
                'customer_id', NEW.customer_id,
                'customer_name', NEW.customer_name,
                'customer_email', NEW.customer_email,
                'total_amount', NEW.total_amount,
                'currency', NEW.currency,
                'status', NEW.status,
                'payment_trigger', jsonb_build_object(
                    'action', 'INITIATE_PAYMENT',
                    'amount', NEW.total_amount,
                    'currency', NEW.currency,
                    'target_account', NEW.customer_id
                )
            )
        ),
        'PENDING',
        NEW.created_at
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 5. Trigger Definition
CREATE OR REPLACE TRIGGER trg_order_created_outbox
AFTER INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION trigger_order_created_outbox();

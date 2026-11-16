CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1. Orders
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number VARCHAR(64) UNIQUE NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    customer_name VARCHAR(128) NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    status VARCHAR(32) NOT NULL DEFAULT 'CREATED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Order Items
CREATE TABLE IF NOT EXISTS order_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    subtotal NUMERIC(12, 2) NOT NULL CHECK (subtotal >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Transactional Outbox
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,

    status VARCHAR(32) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED')),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,

    locked_by VARCHAR(128),
    locked_until TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
ON outbox(created_at)
WHERE status IN ('PENDING', 'FAILED');

CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
ON outbox(aggregate_type, aggregate_id);

-- 4. Payments
CREATE TABLE IF NOT EXISTS payments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'INITIATED'
        CHECK (status IN (
            'INITIATED',
            'PROCESSING',
            'SUCCESS',
            'FAILED',
            'UNKNOWN'
        )),
    gateway_transaction_id VARCHAR(128) UNIQUE,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id
ON payments(order_id);

-- 5. Processed Events
CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    consumer_name VARCHAR(128) NOT NULL,
    aggregate_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_aggregate
ON processed_events(aggregate_id);

-- 6. Aggregate Processing State
CREATE TABLE IF NOT EXISTS aggregate_processing_state (
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id UUID NOT NULL,
    is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
    blocked_event_id UUID,
    blocked_reason TEXT,
    blocked_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (aggregate_type, aggregate_id)
);

-- 7. Outbox Trigger Function
-- Automatically creates an outbox event whenever an order is inserted.
-- Directs the event to the 'orders.created' Kafka topic.
CREATE OR REPLACE FUNCTION trigger_order_created_outbox()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO outbox (
        aggregate_type,
        aggregate_id,
        event_type,
        topic,
        payload,
        status,
        created_at
    ) VALUES (
        'ORDER',
        NEW.id,
        'OrderCreated',
        'orders.created',
        jsonb_build_object(
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
        ),
        'PENDING',
        NEW.created_at
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 8. Trigger Definition
DROP TRIGGER IF EXISTS trg_order_created_outbox ON orders;
CREATE TRIGGER trg_order_created_outbox
AFTER INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION trigger_order_created_outbox();

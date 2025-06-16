CREATE TABLE IF NOT EXISTS orders
(
    order_id         UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    shopping_cart_id UUID NOT NULL,
    username         VARCHAR NOT NULL,
    payment_id       UUID,
    delivery_id      UUID,
    state            VARCHAR,
    delivery_weight  DOUBLE PRECISION,
    delivery_volume  DOUBLE PRECISION,
    fragile          BOOLEAN,
    total_price      DOUBLE PRECISION,
    delivery_price   DOUBLE PRECISION,
    product_price    DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS order_items
(
    order_id   UUID REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id UUID NOT NULL,
    quantity   BIGINT
);
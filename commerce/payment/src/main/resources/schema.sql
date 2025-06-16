CREATE TABLE IF NOT EXISTS payments
(
    payment_id     UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    order_id       UUID NOT NULL,
    delivery_total DOUBLE PRECISION NOT NULL,
    total_payment  DOUBLE PRECISION NOT NULL,
    fee_total      DOUBLE PRECISION NOT NULL,
    payment_state  VARCHAR
);
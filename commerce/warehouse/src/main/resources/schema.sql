CREATE TABLE IF NOT EXISTS warehouse_products (
    product_id       UUID PRIMARY KEY,
    fragile          boolean,
    width            DOUBLE PRECISION,
    height           DOUBLE PRECISION,
    depth            DOUBLE PRECISION,
    weight           DOUBLE PRECISION,
    quantity         BIGINT
);
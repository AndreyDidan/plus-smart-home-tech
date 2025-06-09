CREATE TABLE IF NOT EXISTS products
(
    product_id       UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    product_name     VARCHAR NOT NULL,
    description      VARCHAR NOT NULL,
    image_src        VARCHAR,
    price            DOUBLE PRECISION,
    quantity_state   VARCHAR NOT NULL,
    product_state    VARCHAR NOT NULL,
    product_category VARCHAR NOT NULL,
    rating           INTEGER
);
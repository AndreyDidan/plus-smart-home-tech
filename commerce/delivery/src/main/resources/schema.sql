CREATE TABLE IF NOT EXISTS address
(
    address_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    country    VARCHAR NOT NULL,
    city       VARCHAR NOT NULL,
    street     VARCHAR NOT NULL,
    house      VARCHAR NOT NULL,
    flat       VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveryes
(
    delivery_id      UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    order_id         UUID NOT NULL,
    from_address_id  UUID NOT NULL,
    to_address_id    UUID NOT NULL,
    state            VARCHAR NOT NULL,

    CONSTRAINT delivery_from_address_fk FOREIGN KEY (from_address_id) REFERENCES address(address_id) ON DELETE CASCADE,
    CONSTRAINT delivery_to_address_fk FOREIGN KEY (to_address_id) REFERENCES address(address_id)
);
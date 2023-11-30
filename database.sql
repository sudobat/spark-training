CREATE TABLE IF NOT EXISTS transactions (
    id serial PRIMARY KEY,
    origin VARCHAR(50) NOT NULL,
    destination VARCHAR(50) NOT NULL,
    amount NUMERIC(11, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    message VARCHAR(500) default NULL
);
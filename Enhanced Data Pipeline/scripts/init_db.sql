-- Initialize MySQL with a sample "orders" table and some data
CREATE TABLE IF NOT EXISTS orders (
  order_id INT PRIMARY KEY AUTO_INCREMENT,
  customer_id INT NOT NULL,
  amount DECIMAL(10, 2) NOT NULL,
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO orders (customer_id, amount) VALUES
(1, 100.50),
(2, 250.00),
(3, 75.25),
(4, 500.75),
(5, 13.00);

-- Example dimension table for demonstration (not used by default):
CREATE TABLE IF NOT EXISTS customers (
  customer_id INT PRIMARY KEY,
  customer_name VARCHAR(100),
  join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (customer_id, customer_name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie'),
(4, 'Diana'),
(5, 'Eric');

-- For Postgres, a table "orders_transformed" will be created in the DAG.
-- For MySQL, the table will be created in the "dbt" schema.
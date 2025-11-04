INSERT INTO customers (name) VALUES
  ('Alice'), ('Bob'), ('Charlie');

INSERT INTO products (name, category) VALUES
  ('iPhone', 'phone'),
  ('AirPods', 'audio'),
  ('MacBook', 'laptop'),
  ('iPad', 'tablet'),
  ('Apple Watch', 'watch');

-- Quelques commandes
INSERT INTO orders (customer_id) VALUES (1), (1), (2), (3);

-- Alice commande iPhone + AirPods
INSERT INTO order_items VALUES (1, 1, 1), (1, 2, 1);
-- Alice commande MacBook
INSERT INTO order_items VALUES (2, 3, 1);
-- Bob commande iPhone + Watch
INSERT INTO order_items VALUES (3, 1, 1), (3, 5, 1);
-- Charlie commande iPad + AirPods
INSERT INTO order_items VALUES (4, 4, 1), (4, 2, 1);

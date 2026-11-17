USE shopdb;

-- INSERT
INSERT INTO customers (name, email, city) VALUES ('Divya', 'divya@example.com', 'Chennai');

-- UPDATE
UPDATE customers SET city = 'Pune' WHERE name = 'Asha';

-- DELETE
DELETE FROM customers WHERE name = 'Ravi';

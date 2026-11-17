-- 1. Database and table
CREATE DATABASE IF NOT EXISTS shopdb;
USE shopdb;

CREATE TABLE IF NOT EXISTS customers (
  id    INT AUTO_INCREMENT PRIMARY KEY,
  name  VARCHAR(100) NOT NULL,
  email VARCHAR(100),
  city  VARCHAR(100)
);

-- 2. Two starting rows (Debezium will pick these up in its first "snapshot")
INSERT INTO customers (name, email, city) VALUES
  ('Asha', 'asha@example.com', 'Bengaluru'),
  ('Ravi', 'ravi@example.com', 'Mumbai');

-- 3. A user for Debezium with the permissions it needs to read the log
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

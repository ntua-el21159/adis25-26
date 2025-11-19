-- data/mariadb/init.sql
-- Base MariaDB initialization

SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

CREATE DATABASE IF NOT EXISTS text2sql_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE text2sql_db;

CREATE TABLE IF NOT EXISTS test_connection (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO test_connection (message) VALUES ('MariaDB initialized successfully');

GRANT ALL PRIVILEGES ON *.* TO 'text2sql_user'@'%';
FLUSH PRIVILEGES;

SELECT '✅ MariaDB base initialization complete' AS status;
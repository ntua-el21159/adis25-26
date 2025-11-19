-- data/mysql/init.sql
-- Base MySQL initialization

-- Set character encoding
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- Create default test database
CREATE DATABASE IF NOT EXISTS text2sql_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE text2sql_db;

-- Test table
CREATE TABLE IF NOT EXISTS test_connection (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO test_connection (message) VALUES ('MySQL initialized successfully');

-- Grant permissions
GRANT ALL PRIVILEGES ON *.* TO 'text2sql_user'@'%';
FLUSH PRIVILEGES;

SELECT '✅ MySQL base initialization complete' AS status;
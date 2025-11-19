
-- Yelp Reviews Database
-- Source: text2sql-data repository

CREATE DATABASE IF NOT EXISTS yelp;
USE yelp;

CREATE TABLE IF NOT EXISTS business (
    bid INT PRIMARY KEY,
    business_id VARCHAR(50),
    name VARCHAR(255),
    full_address TEXT,
    city VARCHAR(100),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    review_count INT,
    stars DECIMAL(2,1),
    state VARCHAR(5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS category (
    id INT PRIMARY KEY AUTO_INCREMENT,
    business_id VARCHAR(50),
    category_name VARCHAR(100),
    FOREIGN KEY (business_id) REFERENCES business(business_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user (
    uid INT PRIMARY KEY,
    user_id VARCHAR(50),
    name VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS checkin (
    cid INT PRIMARY KEY,
    business_id VARCHAR(50),
    count INT,
    day VARCHAR(10),
    FOREIGN KEY (business_id) REFERENCES business(business_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS neighbourhood (
    id INT PRIMARY KEY AUTO_INCREMENT,
    business_id VARCHAR(50),
    neighbourhood_name VARCHAR(100),
    FOREIGN KEY (business_id) REFERENCES business(business_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS review (
    rid INT PRIMARY KEY,
    business_id VARCHAR(50),
    user_id VARCHAR(50),
    rating DECIMAL(2,1),
    text TEXT,
    year INT,
    month VARCHAR(10),
    FOREIGN KEY (business_id) REFERENCES business(business_id),
    FOREIGN KEY (user_id) REFERENCES user(user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tip (
    tip_id INT PRIMARY KEY AUTO_INCREMENT,
    business_id VARCHAR(50),
    user_id VARCHAR(50),
    likes INT,
    text TEXT,
    year INT,
    month VARCHAR(10),
    FOREIGN KEY (business_id) REFERENCES business(business_id),
    FOREIGN KEY (user_id) REFERENCES user(user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Sample data
INSERT INTO business (bid, business_id, name, city, stars, review_count, state) VALUES
(1, 'B001', 'Restaurant A', 'Phoenix', 4.5, 150, 'AZ'),
(2, 'B002', 'Cafe B', 'Las Vegas', 4.0, 89, 'NV');

INSERT INTO user (uid, user_id, name) VALUES
(1, 'U001', 'John Doe'),
(2, 'U002', 'Jane Smith');

SELECT 'Yelp database initialized' AS status;


-- Internet Movie Database (IMDB)
-- Source: text2sql-data repository

CREATE DATABASE IF NOT EXISTS imdb;
USE imdb;

CREATE TABLE IF NOT EXISTS actors (
    aid INT PRIMARY KEY,
    gender VARCHAR(10),
    name VARCHAR(255),
    nationality VARCHAR(100),
    birth_city VARCHAR(100),
    birth_year INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS movies (
    mid INT PRIMARY KEY,
    title VARCHAR(255),
    release_year INT,
    title_aka VARCHAR(255),
    budget DECIMAL(15,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS directors (
    did INT PRIMARY KEY,
    gender VARCHAR(10),
    name VARCHAR(255),
    nationality VARCHAR(100),
    birth_city VARCHAR(100),
    birth_year INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS directors_genres (
    did INT,
    genre VARCHAR(50),
    prob DECIMAL(5,4),
    PRIMARY KEY (did, genre),
    FOREIGN KEY (did) REFERENCES directors(did)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS movies_directors (
    did INT,
    mid INT,
    PRIMARY KEY (did, mid),
    FOREIGN KEY (did) REFERENCES directors(did),
    FOREIGN KEY (mid) REFERENCES movies(mid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS movies_genres (
    mid INT,
    genre VARCHAR(50),
    PRIMARY KEY (mid, genre),
    FOREIGN KEY (mid) REFERENCES movies(mid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS roles (
    aid INT,
    mid INT,
    role_name VARCHAR(255),
    PRIMARY KEY (aid, mid),
    FOREIGN KEY (aid) REFERENCES actors(aid),
    FOREIGN KEY (mid) REFERENCES movies(mid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Sample data
INSERT INTO directors (did, name, nationality, birth_year) VALUES
(1, 'Christopher Nolan', 'British', 1970),
(2, 'Quentin Tarantino', 'American', 1963);

INSERT INTO movies (mid, title, release_year, budget) VALUES
(1, 'Inception', 2010, 160000000),
(2, 'Pulp Fiction', 1994, 8000000);

INSERT INTO actors (aid, name, nationality, birth_year) VALUES
(1, 'Leonardo DiCaprio', 'American', 1974),
(2, 'John Travolta', 'American', 1954);

SELECT 'IMDB database initialized' AS status;

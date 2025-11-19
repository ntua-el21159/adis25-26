
-- Academic Publications Database
-- Source: text2sql-data repository

CREATE DATABASE IF NOT EXISTS academic;
USE academic;

CREATE TABLE IF NOT EXISTS author (
    aid INT PRIMARY KEY,
    homepage VARCHAR(255),
    name VARCHAR(255),
    oid INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS conference (
    cid INT PRIMARY KEY,
    homepage VARCHAR(255),
    name VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS domain (
    did INT PRIMARY KEY,
    name VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS domain_author (
    aid INT,
    did INT,
    PRIMARY KEY (aid, did),
    FOREIGN KEY (aid) REFERENCES author(aid),
    FOREIGN KEY (did) REFERENCES domain(did)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS domain_conference (
    cid INT,
    did INT,
    PRIMARY KEY (cid, did),
    FOREIGN KEY (cid) REFERENCES conference(cid),
    FOREIGN KEY (did) REFERENCES domain(did)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS domain_journal (
    did INT,
    jid INT,
    PRIMARY KEY (did, jid),
    FOREIGN KEY (did) REFERENCES domain(did),
    FOREIGN KEY (jid) REFERENCES journal(jid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS domain_keyword (
    did INT,
    kid INT,
    PRIMARY KEY (did, kid),
    FOREIGN KEY (did) REFERENCES domain(did),
    FOREIGN KEY (kid) REFERENCES keyword(kid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS domain_publication (
    did INT,
    pid INT,
    PRIMARY KEY (did, pid),
    FOREIGN KEY (did) REFERENCES domain(did),
    FOREIGN KEY (pid) REFERENCES publication(pid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS journal (
    homepage VARCHAR(255),
    jid INT PRIMARY KEY,
    name VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS keyword (
    keyword VARCHAR(255),
    kid INT PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS organization (
    continent VARCHAR(255),
    homepage VARCHAR(255),
    name VARCHAR(255),
    oid INT PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS publication (
    abstract TEXT,
    cid INT,
    citation_num INT,
    jid INT,
    pid INT PRIMARY KEY,
    reference_num INT,
    title TEXT,
    year INT,
    FOREIGN KEY (cid) REFERENCES conference(cid),
    FOREIGN KEY (jid) REFERENCES journal(jid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS publication_keyword (
    kid INT,
    pid INT,
    PRIMARY KEY (kid, pid),
    FOREIGN KEY (kid) REFERENCES keyword(kid),
    FOREIGN KEY (pid) REFERENCES publication(pid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS writes (
    aid INT,
    pid INT,
    PRIMARY KEY (aid, pid),
    FOREIGN KEY (aid) REFERENCES author(aid),
    FOREIGN KEY (pid) REFERENCES publication(pid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Sample data (μπορείς να προσθέσεις περισσότερα)
INSERT INTO domain (did, name) VALUES
(1, 'Machine Learning'),
(2, 'Database Systems'),
(3, 'Computer Vision');

INSERT INTO keyword (kid, keyword) VALUES
(1, 'neural networks'),
(2, 'SQL'),
(3, 'image processing');

SELECT 'Academic database initialized' AS status;

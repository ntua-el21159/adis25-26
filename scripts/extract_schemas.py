"""
scripts/extract_schemas.py
Extract database schemas από text2sql datasets και δημιούργησε SQL files
"""

import json
import re
from pathlib import Path
from collections import defaultdict

class SchemaExtractor:
    """Extract schemas από text2sql datasets"""
    
    def __init__(self, dataset_path):
        self.dataset_path = Path(dataset_path)
        self.schemas = {}
    
    def extract_from_json(self, json_file):
        """Extract schema από JSON dataset"""
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Analyze SQL queries για schema inference
        tables = defaultdict(set)
        
        for item in data:
            sql = item.get('sql') or item.get('query') or item.get('query_sql', '')
            
            # Extract table names (basic regex)
            # FROM clause
            from_matches = re.findall(r'FROM\s+(\w+)', sql, re.IGNORECASE)
            for table in from_matches:
                tables[table.lower()].add('inferred_from_FROM')
            
            # JOIN clause
            join_matches = re.findall(r'JOIN\s+(\w+)', sql, re.IGNORECASE)
            for table in join_matches:
                tables[table.lower()].add('inferred_from_JOIN')
        
        return dict(tables)
    
    def generate_sql_schema(self, dataset_name, tables_info):
        """Generate CREATE TABLE statements"""
        
        # Manual schemas based on dataset documentation
        # You'll need to create these based on the actual schemas
        
        schemas = {
            'academic': self._academic_schema(),
            'imdb': self._imdb_schema(),
            'yelp': self._yelp_schema()
        }
        
        return schemas.get(dataset_name, self._generic_schema(tables_info))
    
    def _academic_schema(self):
        """Academic dataset schema"""
        return """
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
"""
    
    def _imdb_schema(self):
        """IMDB dataset schema"""
        return """
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
"""
    
    def _yelp_schema(self):
        """Yelp dataset schema"""
        return """
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
"""
    
    def _generic_schema(self, tables_info):
        """Generic schema για unknown datasets"""
        schema = "-- Generic schema\n\n"
        
        for table, info in tables_info.items():
            schema += f"""
CREATE TABLE IF NOT EXISTS {table} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    data TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
        
        return schema

def main():
    """Generate SQL schema files"""
    print("🔧 Schema Extraction Tool")
    print("="*60)
    
    extractor = SchemaExtractor("datasets_source/data")
    
    datasets = ['academic', 'imdb', 'yelp']
    
    for dataset_name in datasets:
        print(f"\n📊 Processing: {dataset_name}")
        
        # Generate schema
        schema_sql = extractor.generate_sql_schema(dataset_name, {})
        
        # Save για MySQL
        mysql_path = Path(f"data/mysql/{dataset_name}.sql")
        mysql_path.parent.mkdir(parents=True, exist_ok=True)
        with open(mysql_path, 'w', encoding='utf-8') as f:
            f.write(schema_sql)
        print(f"   ✅ MySQL schema: {mysql_path}")
        
        # Save για MariaDB (ίδιο schema)
        mariadb_path = Path(f"data/mariadb/{dataset_name}.sql")
        mariadb_path.parent.mkdir(parents=True, exist_ok=True)
        with open(mariadb_path, 'w', encoding='utf-8') as f:
            f.write(schema_sql)
        print(f"   ✅ MariaDB schema: {mariadb_path}")
    
    print(f"\n✅ Schema extraction complete!")
    print(f"📁 MySQL schemas: data/mysql/")
    print(f"📁 MariaDB schemas: data/mariadb/")

if __name__ == "__main__":
    main()
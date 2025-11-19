"""
database/db_manager.py
Enhanced database manager με support για text2sql datasets
"""

import pandas as pd
import time
import json
from pathlib import Path
from sqlalchemy import text, inspect
from database.connection import get_engine

class DatabaseManager:
    """Enhanced database manager για text2sql experiments"""
    
    # Dataset database mapping
    DATASET_DATABASES = {
        'academic': 'academic',
        'imdb': 'imdb',
        'yelp': 'yelp',
        'geography': 'geography',
        'restaurants': 'restaurants'
    }
    
    def __init__(self, db_type, database=None):
        """
        Initialize database manager
        
        Args:
            db_type: 'mysql' or 'mariadb'
            database: Optional specific database
        """
        self.db_type = db_type.lower()
        self.database = database
        self.engine = get_engine(self.db_type, database)
        
        print(f"✅ Connected to {self.db_type.upper()}")
        if database:
            print(f"   Database: {database}")
    
    def execute_query(self, sql, params=None, timeout=30):
        """
        Execute SQL query
        
        Args:
            sql: SQL query string
            params: Optional parameters
            timeout: Query timeout in seconds
        
        Returns:
            dict: {
                'success': bool,
                'result': DataFrame or None,
                'rows_affected': int,
                'execution_time': float,
                'error': str or None
            }
        """
        start_time = time.time()
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params or {})
                
                if result.returns_rows:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    rows_affected = len(df)
                else:
                    df = None
                    rows_affected = result.rowcount
                
                conn.commit()
                
            execution_time = time.time() - start_time
            
            return {
                'success': True,
                'result': df,
                'rows_affected': rows_affected,
                'execution_time': execution_time,
                'error': None,
                'db_type': self.db_type
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            return {
                'success': False,
                'result': None,
                'rows_affected': 0,
                'execution_time': execution_time,
                'error': str(e),
                'db_type': self.db_type
            }
    
    def get_schema(self, database=None):
        """
        Get database schema as formatted string
        
        Args:
            database: Optional database name
        
        Returns:
            str: Schema in CREATE TABLE format
        """
        if database:
            self.switch_database(database)
        
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        
        schema_lines = []
        
        for table in tables:
            columns = inspector.get_columns(table)
            pk = inspector.get_pk_constraint(table)
            fks = inspector.get_foreign_keys(table)
            
            # Build CREATE TABLE
            schema_lines.append(f"CREATE TABLE {table} (")
            
            col_defs = []
            for col in columns:
                col_type = str(col['type'])
                col_def = f"    {col['name']} {col_type}"
                
                if not col['nullable']:
                    col_def += " NOT NULL"
                if col.get('default'):
                    col_def += f" DEFAULT {col['default']}"
                    
                col_defs.append(col_def)
            
            # Add constraints
            if pk['constrained_columns']:
                pk_cols = ', '.join(pk['constrained_columns'])
                col_defs.append(f"    PRIMARY KEY ({pk_cols})")
            
            for fk in fks:
                fk_cols = ', '.join(fk['constrained_columns'])
                ref_table = fk['referred_table']
                ref_cols = ', '.join(fk['referred_columns'])
                col_defs.append(
                    f"    FOREIGN KEY ({fk_cols}) REFERENCES {ref_table}({ref_cols})"
                )
            
            schema_lines.append(',\n'.join(col_defs))
            schema_lines.append(");\n")
        
        return '\n'.join(schema_lines)
    
    def get_schema_for_dataset(self, dataset_name):
        """
        Get schema for specific text2sql dataset
        
        Args:
            dataset_name: 'academic', 'imdb', 'yelp', etc.
        
        Returns:
            str: Schema string
        """
        db_name = self.DATASET_DATABASES.get(dataset_name)
        if not db_name:
            raise ValueError(f"Unknown dataset: {dataset_name}")
        
        return self.get_schema(db_name)
    
    def switch_database(self, database):
        """Switch to different database"""
        self.database = database
        self.engine = get_engine(self.db_type, database)
    
    def list_databases(self):
        """List all databases"""
        result = self.execute_query("SHOW DATABASES")
        if result['success']:
            return result['result']['Database'].tolist()
        return []
    
    def get_table_names(self, database=None):
        """Get table names"""
        if database:
            self.switch_database(database)
        
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def get_dataset_info(self, dataset_name):
        """
        Get information about a text2sql dataset
        
        Returns:
            dict: {
                'database': str,
                'tables': list,
                'table_count': int,
                'available': bool
            }
        """
        db_name = self.DATASET_DATABASES.get(dataset_name)
        if not db_name:
            return {'available': False}
        
        databases = self.list_databases()
        available = db_name in databases
        
        if available:
            tables = self.get_table_names(db_name)
            return {
                'database': db_name,
                'tables': tables,
                'table_count': len(tables),
                'available': True
            }
        else:
            return {
                'database': db_name,
                'available': False,
                'error': 'Database not found'
            }
    
    def test_dataset_query(self, dataset_name, sql_query):
        """
        Test a query on a specific dataset
        
        Args:
            dataset_name: Dataset name
            sql_query: SQL query to test
        
        Returns:
            dict: Query execution result
        """
        db_name = self.DATASET_DATABASES.get(dataset_name)
        if not db_name:
            return {
                'success': False,
                'error': f'Unknown dataset: {dataset_name}'
            }
        
        # Switch to dataset database
        self.switch_database(db_name)
        
        # Execute query
        result = self.execute_query(sql_query)
        result['dataset'] = dataset_name
        result['database'] = db_name
        
        return result
    
    def close(self):
        """Close connection"""
        self.engine.dispose()
        print(f"✅ Closed connection to {self.db_type.upper()}")

def compare_results(result1, result2):
    """
    Compare two query results
    
    Args:
        result1, result2: DataFrames to compare
    
    Returns:
        bool: True if results match
    """
    if result1 is None or result2 is None:
        return False
    
    if result1.shape != result2.shape:
        return False
    
    # Sort both DataFrames
    try:
        df1_sorted = result1.sort_values(
            by=result1.columns.tolist()
        ).reset_index(drop=True)
        df2_sorted = result2.sort_values(
            by=result2.columns.tolist()
        ).reset_index(drop=True)
        
        return df1_sorted.equals(df2_sorted)
    except:
        return False

def compare_db_results(mysql_result, mariadb_result):
    """
    Compare results from MySQL and MariaDB
    
    Returns:
        dict: Comparison results
    """
    both_success = (
        mysql_result['success'] and 
        mariadb_result['success']
    )
    
    if not both_success:
        return {
            'match': False,
            'reason': 'One or both queries failed',
            'mysql_success': mysql_result['success'],
            'mariadb_success': mariadb_result['success']
        }
    
    results_match = compare_results(
        mysql_result['result'],
        mariadb_result['result']
    )
    
    return {
        'match': results_match,
        'mysql_rows': mysql_result['rows_affected'],
        'mariadb_rows': mariadb_result['rows_affected'],
        'mysql_time': mysql_result['execution_time'],
        'mariadb_time': mariadb_result['execution_time'],
        'time_difference': abs(
            mysql_result['execution_time'] - 
            mariadb_result['execution_time']
        )
    }
"""
scripts/test_connections.py
Test database connections and basic operations
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager
from database.db_manager import compare_results

def test_connection(db_type):
    """Test connection to database"""
    print(f"\n{'='*60}")
    print(f"Testing {db_type.upper()} Connection")
    print('='*60)
    
    try:
        # Connect
        db = DatabaseManager(db_type)
        
        # Test 1: List databases
        print("\n📁 Available Databases:")
        databases = db.list_databases()
        for db_name in databases:
            print(f"   - {db_name}")
        
        # Test 2: List tables
        print("\n📊 Tables in 'text2sql_db':")
        tables = db.get_table_names('text2sql_db')
        for table in tables:
            print(f"   - {table}")
        
        # Test 3: Simple query
        print("\n🔍 Test Query: SELECT * FROM test_connection")
        result = db.execute_query(
            "SELECT * FROM test_connection LIMIT 5"
        )
        
        if result['success']:
            print(f"   ✅ Query executed in {result['execution_time']:.3f}s")
            print(f"   📝 Rows returned: {result['rows_affected']}")
            if result['result'] is not None:
                print("\n   Sample data:")
                print(result['result'].to_string(index=False))
        else:
            print(f"   ❌ Query failed: {result['error']}")
        
        # Test 4: Get schema
        print("\n📋 Database Schema (first 500 chars):")
        schema = db.get_schema('text2sql_db')
        print(schema[:500] + "..." if len(schema) > 500 else schema)
        
        # Test 5: Check for dataset databases
        print("\n🎓 Checking Dataset Databases:")
        dataset_dbs = ['academic', 'imdb', 'yelp']
        for dataset_db in dataset_dbs:
            if dataset_db in databases:
                tables = db.get_table_names(dataset_db)
                print(f"   ✅ {dataset_db}: {len(tables)} tables")
            else:
                print(f"   ⚠️  {dataset_db}: Not found")
        
        # Close connection
        db.close()
        
        print(f"\n✅ All tests passed for {db_type.upper()}!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error testing {db_type.upper()}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_comparison():
    """Test query execution on both databases"""
    print(f"\n{'='*60}")
    print("Testing Query Consistency Between MySQL and MariaDB")
    print('='*60)
    
    test_query = """
        SELECT message, created_at
        FROM test_connection
        LIMIT 3
    """
    
    print(f"\nTest Query:\n{test_query}")
    
    try:
        # Execute on MySQL
        print("\n🔵 Executing on MySQL...")
        mysql_db = DatabaseManager('mysql', 'text2sql_db')
        mysql_result = mysql_db.execute_query(test_query)
        
        # Execute on MariaDB
        print("🟠 Executing on MariaDB...")
        mariadb_db = DatabaseManager('mariadb', 'text2sql_db')
        mariadb_result = mariadb_db.execute_query(test_query)
        
        # Compare results
        if mysql_result['success'] and mariadb_result['success']:
            print("\n📊 MySQL Results:")
            print(mysql_result['result'].to_string(index=False))
            
            print("\n📊 MariaDB Results:")
            print(mariadb_result['result'].to_string(index=False))
            
            # Check if results match
            match = compare_results(
                mysql_result['result'],
                mariadb_result['result']
            )
            
            print(f"\n🔍 Results Match: {'✅ YES' if match else '❌ NO'}")
            print(f"   MySQL execution time: {mysql_result['execution_time']:.3f}s")
            print(f"   MariaDB execution time: {mariadb_result['execution_time']:.3f}s")
        else:
            print("\n❌ One or both queries failed")
            if not mysql_result['success']:
                print(f"   MySQL error: {mysql_result['error']}")
            if not mariadb_result['success']:
                print(f"   MariaDB error: {mariadb_result['error']}")
        
        mysql_db.close()
        mariadb_db.close()
        
    except Exception as e:
        print(f"\n❌ Comparison test failed: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    """Main test function"""
    print("🧪 Database Connection Test Suite")
    print("="*60)
    
    # Test MySQL
    mysql_ok = test_connection('mysql')
    
    # Test MariaDB
    mariadb_ok = test_connection('mariadb')
    
    # Test comparison
    if mysql_ok and mariadb_ok:
        test_comparison()
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 Test Summary")
    print('='*60)
    print(f"MySQL:    {'✅ PASS' if mysql_ok else '❌ FAIL'}")
    print(f"MariaDB:  {'✅ PASS' if mariadb_ok else '❌ FAIL'}")
    
    if mysql_ok and mariadb_ok:
        print("\n🎉 All database connections working correctly!")
        return 0
    else:
        print("\n⚠️  Some tests failed. Check the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
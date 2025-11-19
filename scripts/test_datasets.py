"""
scripts/test_datasets.py
Test database setup με text2sql datasets
"""

import sys
from pathlib import Path
from database.db_manager import DatabaseManager, compare_db_results

def test_dataset(dataset_name, db_type='mysql'):
    """Test a specific dataset"""
    print(f"\n{'='*60}")
    print(f"Testing Dataset: {dataset_name} on {db_type.upper()}")
    print('='*60)
    
    db = DatabaseManager(db_type)
    
    # Get dataset info
    info = db.get_dataset_info(dataset_name)
    
    if not info['available']:
        print(f"❌ Dataset '{dataset_name}' not available")
        print(f"   Error: {info.get('error', 'Unknown error')}")
        return False
    
    print(f"✅ Dataset available: {info['database']}")
    print(f"   Tables: {', '.join(info['tables'])}")
    print(f"   Table count: {info['table_count']}")
    
    # Get schema
    print(f"\n📋 Schema:")
    schema = db.get_schema_for_dataset(dataset_name)
    print(schema[:500] + "..." if len(schema) > 500 else schema)
    
    # Test simple query
    print(f"\n🔍 Testing simple query...")
    first_table = info['tables'][0] if info['tables'] else None
    
    if first_table:
        test_query = f"SELECT * FROM {first_table} LIMIT 3"
        result = db.test_dataset_query(dataset_name, test_query)
        
        if result['success']:
            print(f"   ✅ Query successful ({result['execution_time']:.3f}s)")
            print(f"   Rows: {result['rows_affected']}")
            if result['result'] is not None:
                print(f"\n   Sample data:")
                print(result['result'].to_string(index=False))
        else:
            print(f"   ❌ Query failed: {result['error']}")
    
    db.close()
    return True

def test_cross_database_consistency(dataset_name):
    """Test query consistency between MySQL and MariaDB"""
    print(f"\n{'='*60}")
    print(f"Cross-Database Test: {dataset_name}")
    print('='*60)
    
    # Get first table
    db_temp = DatabaseManager('mysql')
    info = db_temp.get_dataset_info(dataset_name)
    db_temp.close()
    
    if not info['available'] or not info['tables']:
        print(f"❌ Cannot test: dataset not available or no tables")
        return
    
    first_table = info['tables'][0]
    test_query = f"SELECT * FROM {first_table} LIMIT 10"
    
    print(f"\nTest Query: {test_query}")
    
    # Execute on MySQL
    print(f"\n🔵 MySQL...")
    mysql_db = DatabaseManager('mysql')
    mysql_result = mysql_db.test_dataset_query(dataset_name, test_query)
    mysql_db.close()
    
    # Execute on MariaDB
    print(f"🟠 MariaDB...")
    mariadb_db = DatabaseManager('mariadb')
    mariadb_result = mariadb_db.test_dataset_query(dataset_name, test_query)
    mariadb_db.close()
    
    # Compare
    comparison = compare_db_results(mysql_result, mariadb_result)
    
    print(f"\n📊 Comparison Results:")
    print(f"   Results Match: {'✅ YES' if comparison['match'] else '❌ NO'}")
    
    if comparison.get('mysql_rows'):
        print(f"   MySQL rows: {comparison['mysql_rows']}")
        print(f"   MariaDB rows: {comparison['mariadb_rows']}")
        print(f"   MySQL time: {comparison['mysql_time']:.3f}s")
        print(f"   MariaDB time: {comparison['mariadb_time']:.3f}s")

def main():
    """Main test function"""
    print("🧪 Text2SQL Dataset Test Suite")
    print("="*60)
    
    # Test datasets
    datasets = ['academic', 'imdb', 'yelp']
    
    success_count = 0
    for dataset in datasets:
        # Test on MySQL
        if test_dataset(dataset, 'mysql'):
            success_count += 1
        
        # Test on MariaDB
        test_dataset(dataset, 'mariadb')
        
        # Cross-database consistency test
        test_cross_database_consistency(dataset)
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 Test Summary")
    print('='*60)
    print(f"   Datasets tested: {len(datasets)}")
    print(f"   Successful: {success_count}/{len(datasets)}")
    
    if success_count == len(datasets):
        print(f"\n🎉 All datasets loaded successfully!")
        return 0
    else:
        print(f"\n⚠️  Some datasets failed to load")
        return 1

if __name__ == "__main__":
    sys.exit(main())
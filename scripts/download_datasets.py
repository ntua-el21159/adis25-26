"""
scripts/download_datasets.py
Download and analyze text2sql datasets
"""

import os
import json
import requests
from pathlib import Path

# Dataset URLs από το text2sql-data repository
DATASETS = {
    'academic': {
        'url': 'https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/academic.json',
        'description': 'Academic publications database - 196 queries, 8 tables'
    },
    'imdb': {
        'url': 'https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/imdb.json',
        'description': 'Internet Movie Database - 131 queries, 7 tables'
    },
    'yelp': {
        'url': 'https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/yelp.json',
        'description': 'Yelp reviews database - 128 queries, 6 tables'
    },
    'geography': {
        'url': 'https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/geography.json',
        'description': 'US Geography database - 877 queries, 2 tables'
    },
    'restaurants': {
        'url': 'https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/restaurants.json',
        'description': 'Restaurant database (GeoQuery) - 878 queries'
    }
}

def download_dataset(name, url, output_dir):
    """Download dataset από GitHub"""
    output_path = Path(output_dir) / f"{name}.json"
    
    print(f"📥 Downloading {name}...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Save locally
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Saved to {output_path}")
        print(f"   📊 {len(data)} examples")
        
        return data
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def analyze_dataset(data, name):
    """Analyze dataset structure"""
    print(f"\n📊 Analysis: {name}")
    print("="*60)
    
    if not data:
        print("   ⚠️ No data to analyze")
        return
    
    # Sample structure
    sample = data[0] if data else {}
    print(f"   Total examples: {len(data)}")
    print(f"   Keys: {list(sample.keys())}")
    
    # Check for SQL queries
    sql_key = None
    for key in ['sql', 'query', 'query_sql']:
        if key in sample:
            sql_key = key
            break
    
    if sql_key:
        print(f"   SQL key: '{sql_key}'")
        
        # Analyze query complexity
        simple = 0
        medium = 0
        complex_queries = 0
        
        for item in data:
            # Get the raw value first
            raw_sql = item.get(sql_key, '')
            
            # Check if it's a list (some datasets tokenized the SQL)
            if isinstance(raw_sql, list):
                # Join the list into a single string
                sql = " ".join(str(x) for x in raw_sql).upper()
            else:
                # Otherwise treat it as a string
                sql = str(raw_sql).upper()
            
            # Simple heuristics
            join_count = sql.count('JOIN')
            subquery_count = sql.count('SELECT') - 1
            
            if join_count == 0 and subquery_count == 0:
                simple += 1
            elif join_count <= 2 and subquery_count <= 1:
                medium += 1
            else:
                complex_queries += 1
        
        print(f"\n   Complexity Distribution:")
        print(f"      Simple:  {simple} ({simple/len(data)*100:.1f}%)")
        print(f"      Medium:  {medium} ({medium/len(data)*100:.1f}%)")
        print(f"      Complex: {complex_queries} ({complex_queries/len(data)*100:.1f}%)")
    
    # Show sample
    print(f"\n   Sample entry:")
    for key, value in list(sample.items())[:3]:
        value_str = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
        print(f"      {key}: {value_str}")

def main():
    """Main download function"""
    print("🚀 Text2SQL Dataset Downloader")
    print("="*60)
    
    # Create directories
    output_dir = Path("datasets_source/data")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Download each dataset
    results = {}
    for name, info in DATASETS.items():
        print(f"\n{'='*60}")
        print(f"Dataset: {name}")
        print(f"Description: {info['description']}")
        print('='*60)
        
        data = download_dataset(name, info['url'], output_dir)
        if data:
            results[name] = data
            analyze_dataset(data, name)
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 Download Summary")
    print('='*60)
    
    for name, data in results.items():
        print(f"   {name:15s}: {len(data):4d} queries")
    
    print(f"\n✅ Downloaded {len(results)}/{len(DATASETS)} datasets successfully!")
    print(f"📁 Location: {output_dir.absolute()}")
    
    return results

if __name__ == "__main__":
    # Για Windows, ensure proper encoding
    import sys
    if sys.platform == 'win32':
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    
    main()
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.qwen_agent import QwenAgent
from database.db_manager import DatabaseManager

DB_NAME = "advising"
QUESTION = "How many courses are there?"

def main():
    print("1. Fetching schema...")
    db = DatabaseManager("mysql", DB_NAME)
    schema = db.get_compact_schema(DB_NAME, question=QUESTION)
    
    print("2. initializing Agent...")
    agent = QwenAgent()
    
    print("3. Generating SQL...")
    sql = agent.generate_sql(schema, QUESTION)
    
    print("\n" + "="*40)
    print(f"QUESTION: {QUESTION}")
    print(f"SQL:      {sql}")
    print("=" * 40 + "\n")
    
    print("4. Executing...")
    res = db.execute_query(sql)
    
    if res['success']:
        print("✅ Execution Success:")
        print(res['result'])
    else:
        print(f"❌ Execution Failed: {res['error']}")
    
    db.close()

if __name__ == "__main__":
    main()
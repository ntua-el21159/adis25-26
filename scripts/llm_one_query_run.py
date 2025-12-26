import sys
from pathlib import Path

# Add project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.gpt2xl_agent import GPT2XLAgent
from database.db_manager import DatabaseManager

def main():
    # 1) Connect to DB (choose mysql first)
    db = DatabaseManager("mysql", "advising")   # change to 'text2sql_db' if you want
    # 2) Pick one question (manual for now)
    question = "How many courses are there?"
    schema = db.get_compact_schema("advising", question=question, max_tables=10)
    print(schema)
    
    # 3) Generate SQL
    agent = GPT2XLAgent()
    sql = agent.generate_sql(schema, question, max_new_tokens=80)

    print("\nQuestion:")
    print(question)
    print("\nGenerated SQL:")
    print(sql)

    # 4) Execute
    print("\nExecuting...")
    result = db.execute_query(sql)

    if result["success"]:
        print(f"✅ Success in {result['execution_time']:.3f}s")
        if result["result"] is not None:
            print(result["result"].head(10).to_string(index=False))
    else:
        print("❌ Failed")
        print(result["error"])

    db.close()

if __name__ == "__main__":
    main()

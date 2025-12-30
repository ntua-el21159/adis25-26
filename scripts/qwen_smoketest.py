import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.qwen_agent import QwenAgent

def main():
    print("Testing Local Qwen Agent...")
    
    # This triggers the download/load of the model
    agent = QwenAgent()
    
    schema = "Tables: student(id, name)"
    question = "List all students"
    
    print(f"\nPrompting with: '{question}'...")
    sql = agent.generate_sql(schema, question)
    
    print(f"\nOutput:\n{sql}")

if __name__ == "__main__":
    main()
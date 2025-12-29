"""
scripts/run_qwen_baseline.py
Run Qwen locally (1.5B version) on the datasets.
"""

import argparse
import json
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.qwen_agent import QwenAgent  # <--- IMPORT LOCAL AGENT
from database.db_manager import DatabaseManager
from scripts.sql_utils import fill_gold_sql, normalize_pred_sql, compare_results

def _default_out_path(dataset_name: str, rdbms: str) -> Path:
    return Path("results") / f"qwen_baseline_{dataset_name}_{rdbms}.jsonl"

def _pack_exec_result(res):
    if res is None: return None
    return {
        "success": res.get("success"),
        "execution_time_s": res.get("execution_time"),
        "rows": res.get("rows_affected"),
        "error": res.get("error"),
    }

def _results_match(res_a, res_b):
    if not res_a or not res_b: return None
    if not res_a.get("success") or not res_b.get("success"): return None
    if res_a.get("result") is None or res_b.get("result") is None: return None
    return compare_results(res_a.get("result"), res_b.get("result"))

def main() -> int:
    parser = argparse.ArgumentParser()
    # No more --api_url needed!
    parser.add_argument("--dataset", type=str, default="datasets_source/data/advising.json")
    parser.add_argument("--rdbms", type=str, default="mysql", choices=["mysql", "mariadb", "both"])
    parser.add_argument("--limit_entries", type=int, default=5)
    parser.add_argument("--max_tables", type=int, default=12)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()

    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        print(f"❌ Dataset not found: {dataset_path}")
        return 1

    dataset_name = dataset_path.stem
    out_path = Path(args.out) if args.out else _default_out_path(dataset_name, args.rdbms)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"🧪 Qwen Local Baseline | DB: {dataset_name} | Target: {args.rdbms}")
    
    data = json.loads(dataset_path.read_text(encoding="utf-8"))

    # Initialize Local Agent
    agent = QwenAgent()

    mysql_db = DatabaseManager("mysql") if args.rdbms in ["mysql", "both"] else None
    maria_db = DatabaseManager("mariadb") if args.rdbms in ["mariadb", "both"] else None
    schema_helper = DatabaseManager("mysql") # Always use mysql for schema info

    row_id = 0
    with out_path.open("w", encoding="utf-8") as f:
        for entry in data[: args.limit_entries]:
            for sentence in entry.get("sentences", []):
                question = sentence.get("text", "")
                
                # 1. Get Schema
                schema = schema_helper.get_compact_schema(dataset_name, question, args.max_tables)
                
                # 2. Generate SQL (Local)
                print(f"[{row_id}] Generating...", end=" ", flush=True)
                t0 = time.time()
                pred_sql_raw = agent.generate_sql(schema, question)
                gen_time = time.time() - t0
                
                # 3. Normalize
                all_tables = schema_helper.get_table_names(dataset_name)
                pred_sql = normalize_pred_sql(pred_sql_raw, all_tables)

                # 4. Execute
                mysql_res = mysql_db.execute_query(pred_sql) if mysql_db else None
                maria_res = maria_db.execute_query(pred_sql) if maria_db else None
                
                # 5. Compare
                match = _results_match(mysql_res, maria_res) if (mysql_res and maria_res) else None
                
                status = "✅" if (mysql_res and mysql_res['success']) else "❌"
                print(f"Done ({gen_time:.2f}s) {status}")

                record = {
                    "id": row_id,
                    "question": question,
                    "pred_sql": pred_sql,
                    "gen_time": gen_time,
                    "mysql": _pack_exec_result(mysql_res),
                    "mariadb": _pack_exec_result(maria_res),
                    "match": match
                }
                f.write(json.dumps(record) + "\n")
                row_id += 1

    if mysql_db: mysql_db.close()
    if maria_db: maria_db.close()
    schema_helper.close()
    print(f"\n✅ Results saved to: {out_path}")
    return 0

if __name__ == "__main__":
    main()
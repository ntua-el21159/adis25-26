"""
scripts/run_qwen_baseline.py

Run a full reproducible Text2SQL baseline using the local Qwen Agent.
Functionally equivalent to run_gpt2xl_baseline.py for fair comparison.

Features:
- Executes BOTH Predicted SQL and Gold SQL
- Calculates Accuracy (Pred Result == Gold Result)
- Handles Dataset Splits (Easy/Medium/Hard)
"""

import argparse
import json
import sys
import time
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.qwen_agent import QwenAgent
from database.db_manager import DatabaseManager
from scripts.sql_utils import fill_gold_sql, normalize_pred_sql, compare_results

# --- Helper Functions (Identical to GPT-2 script) ---

def get_query_split(entry: dict) -> str:
    return str(entry.get("query-split", ""))

def get_sql_variants(entry: dict) -> list[str]:
    sql_list = entry.get("sql", [])
    if isinstance(sql_list, list):
        return [str(x) for x in sql_list]
    return [str(sql_list)] if sql_list else []

def iter_sentences(entry: dict):
    sentences = entry.get("sentences", [])
    if not isinstance(sentences, list):
        return
    for s in sentences:
        if isinstance(s, dict):
            yield s

def get_sentence_text(sentence: dict) -> str:
    return str(sentence.get("text", ""))

def get_question_split(sentence: dict) -> str:
    return str(sentence.get("question-split", ""))

def get_sentence_variables(sentence: dict) -> dict:
    vars_map = sentence.get("variables", {})
    return vars_map if isinstance(vars_map, dict) else {}

def load_dataset(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Dataset JSON must be a list, got: {type(data)}")
    return data

def _default_out_path(dataset_name: str, rdbms: str) -> Path:
    return Path("results") / f"qwen_baseline_{dataset_name}_{rdbms}.jsonl"

def _pack_exec_result(res: dict | None):
    if res is None:
        return None
    return {
        "success": res.get("success"),
        "execution_time_s": res.get("execution_time"),
        "rows": res.get("rows_affected"),
        "error": res.get("error"),
    }

def _results_match(res_a: dict | None, res_b: dict | None) -> bool | None:
    """Returns True if Pred Result matches Gold Result (Accuracy)."""
    if not res_a or not res_b:
        return None
    if not res_a.get("success") or not res_b.get("success"):
        return None

    df_a = res_a.get("result")
    df_b = res_b.get("result")
    if df_a is None or df_b is None:
        return None

    return compare_results(df_a, df_b)

# --- Main Execution Loop ---

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, default="datasets_source/data/advising.json")
    parser.add_argument("--rdbms", type=str, default="mysql", choices=["mysql", "mariadb", "both"])
    parser.add_argument("--limit_entries", type=int, default=5, help="Number of entries to process")
    parser.add_argument("--max_tables", type=int, default=12)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()

    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        print(f"❌ Dataset not found: {dataset_path}")
        return 1

    dataset_name = dataset_path.stem
    
    # Decide output path
    if args.out.strip():
        out_path = Path(args.out)
    else:
        out_path = _default_out_path(dataset_name, args.rdbms)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"🧪 Qwen Local Baseline (Complex) | DB: {dataset_name} | Target: {args.rdbms}")
    print(f"Output: {out_path}")

    data = load_dataset(dataset_path)

    # 1. Initialize Qwen Agent
    agent = QwenAgent()

    # 2. Database Connections
    schema_helper = DatabaseManager("mysql") # Always use mysql for schema info
    mysql_db = None
    maria_db = None

    if args.rdbms in ("mysql", "both"):
        mysql_db = DatabaseManager("mysql")
    if args.rdbms in ("mariadb", "both"):
        maria_db = DatabaseManager("mariadb")

    # Counters
    row_id = 0
    n_ok_mysql = 0
    n_match_mysql = 0

    with out_path.open("w", encoding="utf-8") as f:
        for entry in data[: args.limit_entries]:
            query_split = get_query_split(entry)
            sql_variants = get_sql_variants(entry)
            gold_sql_first = sql_variants[0] if sql_variants else ""

            for sentence in iter_sentences(entry):
                question_text = get_sentence_text(sentence)
                question_split = get_question_split(sentence)
                question_vars = get_sentence_variables(sentence)

                # A. Get Compact Schema
                schema_compact = schema_helper.get_compact_schema(
                    database=dataset_name,
                    question=question_text,
                    max_tables=args.max_tables,
                )

                # B. Prepare Gold SQL (Executable)
                # This fills in the variables (e.g. "math") into the template
                gold_sql_exec = fill_gold_sql(entry, sentence)

                # C. Generate Qwen SQL
                print(f"[{row_id}] Thinking...", end=" ", flush=True)
                t0 = time.time()
                # Note: QwenAgent does not need max_new_tokens passed here (handled internally)
                pred_sql_raw = agent.generate_sql(schema_compact, question_text)
                gen_time = time.time() - t0

                # D. Normalize
                all_tables = schema_helper.get_table_names(database=dataset_name)
                pred_sql = normalize_pred_sql(pred_sql_raw, all_tables)

                # E. Execute on RDBMS
                mysql_pred = mysql_gold = None
                maria_pred = maria_gold = None

                # --- MySQL Execution ---
                if mysql_db:
                    mysql_db.switch_database(dataset_name)
                    # 1. Run Predicted
                    mysql_pred = mysql_db.execute_query(pred_sql)
                    # 2. Run Gold
                    mysql_gold = mysql_db.execute_query(gold_sql_exec)

                    if mysql_pred.get("success"):
                        n_ok_mysql += 1

                # --- MariaDB Execution ---
                if maria_db:
                    maria_db.switch_database(dataset_name)
                    maria_pred = maria_db.execute_query(pred_sql)
                    maria_gold = maria_db.execute_query(gold_sql_exec)

                # F. Compare Results (The "Complex" Part)
                # Check if Pred DataFrame == Gold DataFrame
                mysql_exec_match = _results_match(mysql_pred, mysql_gold)
                maria_exec_match = _results_match(maria_pred, maria_gold)

                if mysql_exec_match:
                    n_match_mysql += 1

                # G. Save Detailed Record
                record = {
                    "id": row_id,
                    "dataset": dataset_name,
                    "query_split": query_split,
                    "question_text": question_text,
                    
                    "gold_sql_exec": gold_sql_exec,
                    "pred_sql": pred_sql,
                    "gen_time_s": round(gen_time, 4),

                    # Detailed Execution Results
                    "mysql": _pack_exec_result(mysql_pred),
                    "mariadb": _pack_exec_result(maria_pred),
                    "mysql_gold": _pack_exec_result(mysql_gold),
                    "mariadb_gold": _pack_exec_result(maria_gold),

                    # The Critical Metric: Did it match the gold standard?
                    "mysql_pred_vs_gold_match": mysql_exec_match,
                    "mariadb_pred_vs_gold_match": maria_exec_match,
                }
                
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

                # Console Feedback
                status_icon = "✔" if mysql_exec_match else "✘"
                if not mysql_pred.get("success"): status_icon = "ERR"
                
                print(f"Done ({gen_time:.1f}s) | Match: {status_icon}")
                
                row_id += 1

    # Cleanup
    schema_helper.close()
    if mysql_db: mysql_db.close()
    if maria_db: maria_db.close()

    print("\n" + "=" * 50)
    print(f"📊 SUMMARY (Qwen 1.5B)")
    print(f"Total Questions: {row_id}")
    if mysql_db:
        acc = (n_match_mysql / row_id * 100) if row_id > 0 else 0
        print(f"MySQL Accuracy:  {n_match_mysql}/{row_id} ({acc:.1f}%)")
    print(f"Results saved to: {out_path}")
    print("=" * 50)
    return 0

if __name__ == "__main__":
    main()
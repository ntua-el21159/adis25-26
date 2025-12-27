"""
scripts/jsonl_to_csv_metrics.py

Convert JSONL results (one record per question) into CSVs ready for plotting.

Inputs:
  - JSONL produced by scripts/run_gpt2xl_baseline.py

Outputs:
  - Per-question CSV (one row per question): results/*.csv
  - Per-dataset summary CSV: results/*_summary.csv

Metrics supported (all 9 from our list):
  1) Execution Success Rate (pred) per RDBMS
  2) Execution Accuracy (EX) per RDBMS (pred vs gold result match)
  3) Conditional EX | success per RDBMS
  4) Cross-RDBMS result agreement rate (pred) among both-success
  5) Cross-RDBMS failure asymmetry rates (mysql-only, mariadb-only)
  6) Generation time distributions (mean/median/p90 etc. computed in summary)
  7) Execution time distributions (pred exec time)
  8) Accuracy vs query complexity bucket (simple/medium/complex)
  9) Schema size sensitivity (tables_in_schema_compact) + max_tables from args (if present)

Notes:
  - Assumes baseline JSONL contains fields like:
      pred_sql, gen_time_s, schema_compact,
      mysql / mariadb objects for pred execution,
      mysql_gold / mariadb_gold objects for gold execution,
      mysql_pred_vs_gold_match / mariadb_pred_vs_gold_match,
      mysql_vs_mariadb_match
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


# -----------------------------
# Helpers: parsing / flattening
# -----------------------------

def _safe_get(d: Optional[dict], key: str, default=None):
    if isinstance(d, dict):
        return d.get(key, default)
    return default


def _to_bool_or_none(x) -> Optional[bool]:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    # sometimes stored as 0/1, "true"/"false"
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        s = x.strip().lower()
        if s in ("true", "t", "yes", "y", "1"):
            return True
        if s in ("false", "f", "no", "n", "0"):
            return False
    return None


def _parse_schema_compact(schema_compact: str) -> List[str]:
    """
    schema_compact example lines:
      COURSE(COURSE_ID, NAME, ...)
      PROGRAM(program_id, ...)
    Return list of table names.
    """
    if not schema_compact:
        return []
    tables = []
    for line in schema_compact.splitlines():
        line = line.strip()
        if not line:
            continue
        # table_name(...) pattern
        m = re.match(r"^([A-Za-z0-9_]+)\s*\(", line)
        if m:
            tables.append(m.group(1))
    return tables


def _infer_sql_complexity(sql: str) -> str:
    """
    Heuristic complexity bucket:
      simple: no JOIN and no subquery
      medium: <=2 JOIN and <=1 subquery
      complex: otherwise
    """
    if not sql:
        return "unknown"
    s = sql.upper()
    join_count = s.count("JOIN")
    subquery_count = max(0, s.count("SELECT") - 1)

    if join_count == 0 and subquery_count == 0:
        return "simple"
    if join_count <= 2 and subquery_count <= 1:
        return "medium"
    return "complex"


def _quantile(xs: List[float], q: float) -> Optional[float]:
    """Nearest-rank quantile (simple + deterministic)."""
    if not xs:
        return None
    xs_sorted = sorted(xs)
    if q <= 0:
        return xs_sorted[0]
    if q >= 1:
        return xs_sorted[-1]
    k = int(math.ceil(q * len(xs_sorted))) - 1
    k = max(0, min(k, len(xs_sorted) - 1))
    return xs_sorted[k]


def _mean(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    return sum(xs) / len(xs)


def _median(xs: List[float]) -> Optional[float]:
    return _quantile(xs, 0.5)


def _count_not_none(xs: List[Any]) -> int:
    return sum(1 for x in xs if x is not None)


# -----------------------------
# Core: JSONL -> row dict
# -----------------------------

def jsonl_records(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def to_flat_row(rec: dict) -> dict:
    """
    Flatten one JSONL record into a single CSV row with
    computed flags for all required metrics.
    """
    dataset = rec.get("dataset", "")
    rid = rec.get("id", None)

    query_split = rec.get("query_split", "")
    question_split = rec.get("question_split", "")
    question_text = rec.get("question_text", "")

    gen_time_s = rec.get("gen_time_s", None)
    pred_sql = rec.get("pred_sql", rec.get("pred_sql_raw", ""))
    gold_sql_exec = rec.get("gold_sql_exec", "")

    rdbms_mode = rec.get("rdbms_mode", "")

    schema_compact = rec.get("schema_compact", "")
    schema_tables = _parse_schema_compact(schema_compact)
    tables_in_schema_compact = len(schema_tables)

    # Complexity based on GOLD (preferred) else predicted
    complexity = _infer_sql_complexity(gold_sql_exec or rec.get("gold_sql_first", "") or pred_sql)

    # Pred exec objects
    mysql_pred = rec.get("mysql", None)
    maria_pred = rec.get("mariadb", None)

    # Gold exec objects
    mysql_gold = rec.get("mysql_gold", None)
    maria_gold = rec.get("mariadb_gold", None)

    # Pred-vs-gold result match flags (already computed by runner)
    mysql_ex = _to_bool_or_none(rec.get("mysql_pred_vs_gold_match"))
    maria_ex = _to_bool_or_none(rec.get("mariadb_pred_vs_gold_match"))

    # Cross-RDBMS predicted result match (already computed)
    mysql_vs_maria_match = _to_bool_or_none(rec.get("mysql_vs_mariadb_match"))

    # Success flags (pred + gold)
    mysql_pred_success = _to_bool_or_none(_safe_get(mysql_pred, "success"))
    maria_pred_success = _to_bool_or_none(_safe_get(maria_pred, "success"))
    mysql_gold_success = _to_bool_or_none(_safe_get(mysql_gold, "success"))
    maria_gold_success = _to_bool_or_none(_safe_get(maria_gold, "success"))

    # Times
    mysql_pred_exec_time = _safe_get(mysql_pred, "execution_time_s")
    maria_pred_exec_time = _safe_get(maria_pred, "execution_time_s")
    mysql_gold_exec_time = _safe_get(mysql_gold, "execution_time_s")
    maria_gold_exec_time = _safe_get(maria_gold, "execution_time_s")

    # Conditional EX | success (computed per row)
    mysql_ex_given_success = None
    if mysql_pred_success is True:
        mysql_ex_given_success = mysql_ex  # True/False/None

    maria_ex_given_success = None
    if maria_pred_success is True:
        maria_ex_given_success = maria_ex

    # Cross-RDBMS asymmetry flags (pred)
    mysql_only_success = (mysql_pred_success is True) and (maria_pred_success is False)
    maria_only_success = (maria_pred_success is True) and (mysql_pred_success is False)
    both_success = (mysql_pred_success is True) and (maria_pred_success is True)
    neither_success = (mysql_pred_success is False) and (maria_pred_success is False)

    row = {
        # Identity
        "id": rid,
        "dataset": dataset,
        "rdbms_mode": rdbms_mode,

        # Splits
        "query_split": query_split,
        "question_split": question_split,

        # Question
        "question_text": question_text,

        # Prompt / schema
        "tables_in_schema_compact": tables_in_schema_compact,
        "complexity_bucket": complexity,

        # SQL strings
        "pred_sql": pred_sql,
        "gold_sql_exec": gold_sql_exec,

        # Timings
        "gen_time_s": gen_time_s,

        # Pred execution success
        "mysql_pred_success": mysql_pred_success,
        "mariadb_pred_success": maria_pred_success,

        # Gold execution success
        "mysql_gold_success": mysql_gold_success,
        "mariadb_gold_success": maria_gold_success,

        # Pred execution times
        "mysql_pred_execution_time_s": mysql_pred_exec_time,
        "mariadb_pred_execution_time_s": maria_pred_exec_time,

        # Gold execution times
        "mysql_gold_execution_time_s": mysql_gold_exec_time,
        "mariadb_gold_execution_time_s": maria_gold_exec_time,

        # EX (pred vs gold)
        "mysql_ex": mysql_ex,
        "mariadb_ex": maria_ex,

        # EX|success
        "mysql_ex_given_success": mysql_ex_given_success,
        "mariadb_ex_given_success": maria_ex_given_success,

        # Cross-RDBMS predicted result agreement
        "mysql_vs_mariadb_match": mysql_vs_maria_match,

        # Asymmetry
        "pred_mysql_only_success": mysql_only_success,
        "pred_mariadb_only_success": maria_only_success,
        "pred_both_success": both_success,
        "pred_neither_success": neither_success,
    }
    return row


# -----------------------------
# Summary aggregation
# -----------------------------

def summarize(rows: List[dict]) -> List[dict]:
    """
    Produce per-dataset summary rows with rates + timing stats,
    plus breakdown by complexity bucket.
    """
    # group by dataset
    by_dataset: Dict[str, List[dict]] = {}
    for r in rows:
        by_dataset.setdefault(r["dataset"], []).append(r)

    summaries: List[dict] = []

    for dataset, ds_rows in sorted(by_dataset.items()):
        n = len(ds_rows)

        # success counts (pred)
        mysql_pred_succ = sum(1 for r in ds_rows if r["mysql_pred_success"] is True)
        maria_pred_succ = sum(1 for r in ds_rows if r["mariadb_pred_success"] is True)

        # EX counts
        mysql_ex_true = sum(1 for r in ds_rows if r["mysql_ex"] is True)
        maria_ex_true = sum(1 for r in ds_rows if r["mariadb_ex"] is True)

        # EX | success denom excludes None
        mysql_ex_given_success_vals = [r["mysql_ex_given_success"] for r in ds_rows]
        maria_ex_given_success_vals = [r["mariadb_ex_given_success"] for r in ds_rows]

        mysql_ex_gs_denom = sum(1 for x in mysql_ex_given_success_vals if x in (True, False))
        maria_ex_gs_denom = sum(1 for x in maria_ex_given_success_vals if x in (True, False))

        mysql_ex_gs_true = sum(1 for x in mysql_ex_given_success_vals if x is True)
        maria_ex_gs_true = sum(1 for x in maria_ex_given_success_vals if x is True)

        # Cross-RDBMS agreement among both-success where match is comparable
        match_vals = [r["mysql_vs_mariadb_match"] for r in ds_rows if r["pred_both_success"]]
        match_denom = sum(1 for x in match_vals if x in (True, False))
        match_true = sum(1 for x in match_vals if x is True)

        # Asymmetry
        mysql_only = sum(1 for r in ds_rows if r["pred_mysql_only_success"])
        maria_only = sum(1 for r in ds_rows if r["pred_mariadb_only_success"])
        both = sum(1 for r in ds_rows if r["pred_both_success"])
        neither = sum(1 for r in ds_rows if r["pred_neither_success"])

        # Timings
        gen_times = [float(r["gen_time_s"]) for r in ds_rows if r["gen_time_s"] is not None]
        mysql_exec_times = [
            float(r["mysql_pred_execution_time_s"])
            for r in ds_rows
            if r["mysql_pred_execution_time_s"] is not None
        ]
        maria_exec_times = [
            float(r["mariadb_pred_execution_time_s"])
            for r in ds_rows
            if r["mariadb_pred_execution_time_s"] is not None
        ]

        # Schema size
        schema_sizes = [int(r["tables_in_schema_compact"]) for r in ds_rows if r["tables_in_schema_compact"] is not None]

        base = {
            "dataset": dataset,
            "n_questions": n,

            # 1) Execution success rate
            "mysql_pred_success_rate": mysql_pred_succ / n if n else None,
            "mariadb_pred_success_rate": maria_pred_succ / n if n else None,

            # 2) EX overall
            "mysql_execution_accuracy_ex": mysql_ex_true / n if n else None,
            "mariadb_execution_accuracy_ex": maria_ex_true / n if n else None,

            # 3) EX | success
            "mysql_ex_given_success": (mysql_ex_gs_true / mysql_ex_gs_denom) if mysql_ex_gs_denom else None,
            "mariadb_ex_given_success": (maria_ex_gs_true / maria_ex_gs_denom) if maria_ex_gs_denom else None,

            # 4) Cross-RDBMS agreement among both-success
            "mysql_vs_mariadb_match_rate": (match_true / match_denom) if match_denom else None,

            # 5) Failure asymmetry (pred)
            "mysql_only_success_rate": mysql_only / n if n else None,
            "mariadb_only_success_rate": maria_only / n if n else None,
            "both_success_rate": both / n if n else None,
            "neither_success_rate": neither / n if n else None,

            # 6) Generation time stats
            "gen_time_mean_s": _mean(gen_times),
            "gen_time_median_s": _median(gen_times),
            "gen_time_p90_s": _quantile(gen_times, 0.90),

            # 7) Execution time stats
            "mysql_exec_time_mean_s": _mean(mysql_exec_times),
            "mysql_exec_time_median_s": _median(mysql_exec_times),
            "mariadb_exec_time_mean_s": _mean(maria_exec_times),
            "mariadb_exec_time_median_s": _median(maria_exec_times),

            # 9) Schema size stats (for sensitivity)
            "schema_tables_mean": _mean([float(x) for x in schema_sizes]) if schema_sizes else None,
            "schema_tables_median": _median([float(x) for x in schema_sizes]) if schema_sizes else None,
        }

        summaries.append(base)

        # 8) Complexity breakdown rows (optional but handy)
        for bucket in ("simple", "medium", "complex", "unknown"):
            b_rows = [r for r in ds_rows if r["complexity_bucket"] == bucket]
            if not b_rows:
                continue
            bn = len(b_rows)
            summaries.append({
                "dataset": dataset,
                "n_questions": bn,
                "breakdown": "complexity",
                "bucket": bucket,
                "mysql_pred_success_rate": sum(1 for r in b_rows if r["mysql_pred_success"] is True) / bn,
                "mariadb_pred_success_rate": sum(1 for r in b_rows if r["mariadb_pred_success"] is True) / bn,
                "mysql_execution_accuracy_ex": sum(1 for r in b_rows if r["mysql_ex"] is True) / bn,
                "mariadb_execution_accuracy_ex": sum(1 for r in b_rows if r["mariadb_ex"] is True) / bn,
            })

    return summaries


# -----------------------------
# CSV writing
# -----------------------------

def write_csv(path: Path, rows: List[dict]) -> None:
    if not rows:
        raise ValueError("No rows to write.")
    path.parent.mkdir(parents=True, exist_ok=True)

    # stable header: union of keys
    keys = sorted({k for r in rows for k in r.keys()})
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--jsonl",
        type=str,
        required=True,
        help="Path to JSONL results file (e.g., results/gpt2xl_baseline_mysql.jsonl).",
    )
    parser.add_argument(
        "--out_csv",
        type=str,
        default="",
        help="Output CSV path. Default: same as jsonl but .csv",
    )
    parser.add_argument(
        "--out_summary_csv",
        type=str,
        default="",
        help="Output summary CSV path. Default: same as out_csv but *_summary.csv",
    )
    args = parser.parse_args()

    jsonl_path = Path(args.jsonl)
    if not jsonl_path.exists():
        print(f"❌ JSONL not found: {jsonl_path}")
        return 1

    out_csv = Path(args.out_csv) if args.out_csv else jsonl_path.with_suffix(".csv")
    out_summary = (
        Path(args.out_summary_csv)
        if args.out_summary_csv
        else out_csv.with_name(out_csv.stem + "_summary.csv")
    )

    # Build per-question rows
    rows: List[dict] = []
    for rec in jsonl_records(jsonl_path):
        rows.append(to_flat_row(rec))

    write_csv(out_csv, rows)

    # Build summary rows
    summary_rows = summarize(rows)
    write_csv(out_summary, summary_rows)

    print(f"✅ Wrote per-question CSV: {out_csv}")
    print(f"✅ Wrote summary CSV:     {out_summary}")
    print(f"Rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

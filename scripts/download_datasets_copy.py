"""
scripts/download_datasets.py
Download and analyze text2sql datasets (robust + reproducible)
"""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATASETS: Dict[str, Dict[str, str]] = {
    "academic": {
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/academic.json",
        "description": "Academic publications database - 196 queries, 8 tables",
    },
    "imdb": {
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/imdb.json",
        "description": "Internet Movie Database - 131 queries, 7 tables",
    },
    "yelp": {
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/yelp.json",
        "description": "Yelp reviews database - 128 queries, 6 tables",
    },
    "geography": {
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/geography.json",
        "description": "US Geography database - 877 queries, 2 tables",
    },
    "restaurants": {
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/restaurants.json",
        "description": "Restaurant database (GeoQuery)",
    },
    "advising": {
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/advising.json",
        "description": "University advising database",
    },
    "atis": {
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/master/data/atis.json",
        "description": "Airline travel information system",
    },
}

SQL_KEYS = ["sql", "query", "query_sql", "sql_query", "sqls"]


def make_session() -> requests.Session:
    """Requests session with retries for flaky networks."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "adis-llmsql3-dataset-downloader/1.0"})
    return session


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def detect_sql_field(sample: Dict[str, Any]) -> Optional[str]:
    """Return the key that most likely stores SQL, else None."""
    for k in SQL_KEYS:
        if k in sample:
            return k
    return None


def normalize_sql(value: Any) -> str:
    """Convert possibly-tokenized SQL to a single uppercase string."""
    if isinstance(value, list):
        return " ".join(str(x) for x in value).upper()
    return str(value).upper()


def estimate_complexity(sql: str) -> str:
    """Cheap heuristic: simple / medium / complex."""
    join_count = sql.count("JOIN")
    select_count = sql.count("SELECT")
    has_group = "GROUP BY" in sql
    has_having = "HAVING" in sql
    has_set_ops = any(op in sql for op in ("UNION", "INTERSECT", "EXCEPT"))
    subqueries = max(0, select_count - 1)

    score = 0
    score += join_count * 2
    score += subqueries * 3
    score += 2 if has_group else 0
    score += 2 if has_having else 0
    score += 3 if has_set_ops else 0

    if score <= 1:
        return "simple"
    if score <= 6:
        return "medium"
    return "complex"


def analyze_dataset(data: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    """Return analysis dict for manifest."""
    if not data:
        return {"name": name, "total": 0, "sql_key": None, "complexity": {}}

    sample = data[0]
    sql_key = detect_sql_field(sample)

    dist = {"simple": 0, "medium": 0, "complex": 0}
    if sql_key:
        for item in data:
            sql_raw = item.get(sql_key, "")
            sql = normalize_sql(sql_raw)
            dist[estimate_complexity(sql)] += 1

    return {
        "name": name,
        "total": len(data),
        "keys": list(sample.keys()),
        "sql_key": sql_key,
        "complexity": dist,
    }


def download_dataset(
    session: requests.Session,
    name: str,
    url: str,
    output_dir: Path,
    force: bool = False,
) -> Tuple[Optional[List[Dict[str, Any]]], Path, str]:
    """Download dataset from URL unless cached."""
    output_path = output_dir / f"{name}.json"

    if output_path.exists() and not force:
        return load_json(output_path), output_path, "cached"

    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    save_json(output_path, data)
    return data, output_path, "downloaded"


def main() -> int:
    print("🚀 Text2SQL Dataset Downloader")
    print("=" * 60)

    output_dir = Path("datasets_source/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    session = make_session()

    manifest = {
        "generated_at_epoch": int(time.time()),
        "datasets": [],
    }

    ok = 0
    for name, info in DATASETS.items():
        print(f"\n{'='*60}")
        print(f"Dataset: {name}")
        print(f"Description: {info['description']}")
        print("=" * 60)

        try:
            data, path, mode = download_dataset(
                session, name, info["url"], output_dir, force=False
            )
            analysis = analyze_dataset(data or [], name)

            print(f"📁 {mode.upper()}: {path}")
            print(f"📊 Total examples: {analysis['total']}")
            if analysis["sql_key"]:
                c = analysis["complexity"]
                total = max(1, analysis["total"])
                print("   Complexity:")
                print(f"      Simple:  {c['simple']} ({c['simple']/total*100:.1f}%)")
                print(f"      Medium:  {c['medium']} ({c['medium']/total*100:.1f}%)")
                print(f"      Complex: {c['complex']} ({c['complex']/total*100:.1f}%)")
            else:
                print("⚠️  Could not detect SQL field key in dataset entries.")

            manifest["datasets"].append(
                {
                    "name": name,
                    "url": info["url"],
                    "description": info["description"],
                    "file": str(path.as_posix()),
                    "mode": mode,
                    **analysis,
                }
            )

            ok += 1
        except Exception as e:
            print(f"❌ Failed for {name}: {e}")

    save_json(Path("datasets_source/manifest.json"), manifest)

    print(f"\n✅ Finished: {ok}/{len(DATASETS)} datasets available")
    print(f"🧾 Manifest: {Path('datasets_source/manifest.json').absolute()}")
    return 0 if ok > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

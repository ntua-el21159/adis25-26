import argparse
import os
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# -----------------------------
# Config: container names match docker-compose.yml
# -----------------------------
CONTAINERS = {
    "mysql": "text2sql-mysql",
    "mariadb": "text2sql-mariadb",
}

# -----------------------------
# Where to cache downloaded SQL assets
# -----------------------------
CACHE_DIR = Path("data/source_sql")
SCHEMA_OUT_DIR = Path("data/processed/schemas")

# -----------------------------
# SQL dump sources
# You can keep them empty for now; script will skip missing.
# -----------------------------
DATASET_SQL_SOURCES: Dict[str, Dict] = {
    # direct .sql
    "advising": {
        "type": "direct_sql",
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/refs/heads/master/data/advising-db.sql",
        "out_name": "advising.sql",
    },

    "atis": {
        "type": "direct_sql",
        "url": "https://raw.githubusercontent.com/jkkummerfeld/text2sql-data/refs/heads/master/data/atis-db.sql",
        "out_name": "atis.sql",
    },


    # TODO: add more datasets here
    # download .tgz from drive (https://drive.google.com/file/d/11qRUfkEVj7Lapa9ypPfwrDGUFsJRsVx9/view?usp=sharing)
    # extract all .sql files
    # and pick the right one for each dataset
    # Example: zip with members (e.g., Google Drive zip)
    # "academic": {
    #     "type": "zip",
    #     "url": "https://example.com/mas_imdb_yelp.zip",
    #     "zip_name": "mas_imdb_yelp.zip",
    #     "member": "academic-db.sql",
    #     "out_name": "academic.sql",
    # },
}

# If user doesn't specify, these are the usual LLMSQL3 datasets
DEFAULT_DATASETS = ["academic", "imdb", "yelp", "advising", "atis"]


@dataclass
class DbCreds:
    root_password: str


def run(cmd: List[str], *, input_path: Optional[Path] = None) -> None:
    """Run a subprocess command, optionally piping a file to stdin."""
    if input_path is None:
        subprocess.run(cmd, check=True)
    else:
        with input_path.open("rb") as f:
            subprocess.run(cmd, check=True, stdin=f)


def download_file(url: str, out_path: Path, force: bool = False) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and not force:
        print(f"📦 Using cached: {out_path}")
        return out_path

    print(f"⬇️  Downloading: {url}")
    resp = requests.get(url, timeout=60, stream=True)
    resp.raise_for_status()

    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    print(f"✅ Saved: {out_path}")
    return out_path


def resolve_sql_asset(dataset: str, force_download: bool) -> Optional[Path]:
    """
    Return local path to the dataset SQL file, downloading if needed.
    Returns None if no source is configured (yet).
    """
    if dataset not in DATASET_SQL_SOURCES:
        print(f"⚠️  No SQL source configured for dataset '{dataset}'. Skipping import.")
        return None

    src = DATASET_SQL_SOURCES[dataset]
    typ = src.get("type")

    if typ == "direct_sql":
        url = src["url"]
        out_name = src.get("out_name", f"{dataset}.sql")
        return download_file(url, CACHE_DIR / out_name, force=force_download)

    if typ == "zip":
        url = src["url"]
        zip_name = src.get("zip_name", f"{dataset}.zip")
        member = src["member"]
        out_name = src.get("out_name", f"{dataset}.sql")

        zip_path = download_file(url, CACHE_DIR / zip_name, force=force_download)

        # Extract single member
        out_path = CACHE_DIR / out_name
        if out_path.exists() and not force_download:
            print(f"📦 Using cached extracted SQL: {out_path}")
            return out_path

        print(f"🗜️  Extracting '{member}' from {zip_path.name}")
        with zipfile.ZipFile(zip_path, "r") as z:
            if member not in z.namelist():
                raise RuntimeError(
                    f"Member '{member}' not found in zip. Available: {z.namelist()[:20]}..."
                )
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with z.open(member) as src_f, out_path.open("wb") as dst_f:
                dst_f.write(src_f.read())

        print(f"✅ Extracted: {out_path}")
        return out_path

    raise ValueError(f"Unknown SQL source type: {typ}")


def docker_mysql_exec(db_type: str, creds: DbCreds, sql: str) -> None:
    """Execute a one-liner SQL command inside the container."""
    container = CONTAINERS[db_type]
    # mysql client exists in both mysql and mariadb images
    client = "mysql" if db_type == "mysql" else "mariadb"
    cmd = [
    "docker", "exec", "-i", container,
    client, "-uroot", f"-p{creds.root_password}",
    "-e", sql
    ]
    run(cmd)


def docker_mysql_import_file(db_type: str, creds: DbCreds, dataset_db: str, sql_file: Path) -> None:
    """
    Import a .sql file into a specific database using docker exec + mysql stdin.
    We call: mysql -uroot -p... <db> < file.sql
    """
    container = CONTAINERS[db_type]
    client = "mysql" if db_type == "mysql" else "mariadb"
    cmd = [
    "docker", "exec", "-i", container,
    client, "-uroot", f"-p{creds.root_password}", dataset_db
    ]
    run(cmd, input_path=sql_file)


def ensure_db(db_type: str, creds: DbCreds, db_name: str, reset: bool) -> None:
    if reset:
        print(f"🧹 Dropping database '{db_name}' on {db_type} (if exists)")
        docker_mysql_exec(db_type, creds, f"DROP DATABASE IF EXISTS `{db_name}`;")
    print(f"🛠️  Creating database '{db_name}' on {db_type} (if not exists)")
    docker_mysql_exec(
        db_type, creds,
        f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )


def extract_schema_snapshot(db_type: str, creds: DbCreds, db_name: str) -> None:
    """
    Dump schema-only (no data) using mysqldump.
    Helpful for later prompts & debugging.
    """
    container = CONTAINERS[db_type]
    out_dir = SCHEMA_OUT_DIR / db_type
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{db_name}.schema.sql"

    dump = "mysqldump" if db_type == "mysql" else "mariadb-dump"
    cmd = [
    "docker", "exec", "-i", container,
    dump, "-uroot", f"-p{creds.root_password}",
    "--no-data", "--routines", "--triggers",
    db_name
    ]


    print(f"🧾 Writing schema snapshot: {out_path}")
    with out_path.open("wb") as f:
        subprocess.run(cmd, check=True, stdout=f)


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Download SQL dumps (if available), import into MySQL/MariaDB, and extract schema snapshots.")
    parser.add_argument("--datasets", nargs="+", default=DEFAULT_DATASETS, help="Dataset DBs to import (default: academic imdb yelp)")
    parser.add_argument("--only", choices=["mysql", "mariadb"], default=None, help="Only run for one RDBMS")
    parser.add_argument("--reset-db", action="store_true", help="Drop dataset DBs before importing")
    parser.add_argument("--force-download", action="store_true", help="Redownload SQL assets even if cached")
    parser.add_argument("--no-schema-dump", action="store_true", help="Skip schema snapshot extraction")
    args = parser.parse_args()

    # Root passwords from .env
    mysql_creds = DbCreds(root_password=os.getenv("MYSQL_ROOT_PASSWORD", "root123"))
    mariadb_creds = DbCreds(root_password=os.getenv("MARIADB_ROOT_PASSWORD", "root123"))

    targets = ["mysql", "mariadb"] if args.only is None else [args.only]

    print("🚀 DB Bootstrapper (SQL import + schema snapshots)")
    print(f"Targets: {targets}")
    print(f"Datasets: {args.datasets}")
    print(f"Reset DBs: {args.reset_db}")
    print(f"Force download: {args.force_download}")
    print("")

    for db_type in targets:
        creds = mysql_creds if db_type == "mysql" else mariadb_creds
        print("=" * 70)
        print(f"🔧 RDBMS: {db_type.upper()}")
        print("=" * 70)

        for dataset in args.datasets:
            print(f"\n--- Dataset: {dataset} ---")
            sql_path = resolve_sql_asset(dataset, force_download=args.force_download)
            if sql_path is None:
                continue

            ensure_db(db_type, creds, dataset, reset=args.reset_db)

            print(f"📥 Importing {sql_path.name} into {db_type}:{dataset} ...")
            try:
                docker_mysql_import_file(db_type, creds, dataset, sql_path)
                print(f"✅ Import complete: {db_type}:{dataset}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Import failed for {db_type}:{dataset}.")
                print("Tip: check container logs and SQL syntax compatibility.")
                return 1

            if not args.no_schema_dump:
                extract_schema_snapshot(db_type, creds, dataset)

    print("\n✅ Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

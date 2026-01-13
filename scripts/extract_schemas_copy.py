import argparse
import os
import re
import shutil
import subprocess
import tarfile
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

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

# New: where to cache bundles
ARCHIVES_DIR = Path("data/source_sql_archives")
EXTRACTED_DIR = Path("data/source_sql_extracted")
QUESTIONS_DIR = Path("data/questions")

# -----------------------------
# Google Drive TGZ bundle
# -----------------------------
SQL_BUNDLES: Dict[str, Dict] = {
    "sqlizer": {
        "type": "tgz",
        # You can also use drive.usercontent.google.com/download?... if you prefer.
        "url": "https://drive.google.com/uc?export=download&id=11qRUfkEVj7Lapa9ypPfwrDGUFsJRsVx9",
        "archive_name": "sqlizer.tgz",
        "extract_dir": EXTRACTED_DIR / "sqlizer",
        # Mapping: dataset -> SQL file inside tgz (filename anywhere in extracted tree)
        "sql_members": {
            "academic": "MAS.database.sql",
            "imdb": "IMDB.database.sql",
            "yelp": "YELP.database.sql",
        },
        # Mapping: dataset -> questions file inside tgz
        "questions_members": {
            "academic": "MAS.questions.txt",
            "imdb": "IMDB.questions.txt",
            "yelp": "YELP.questions.txt",
        },
    }
}

# -----------------------------
# SQL dump sources
# -----------------------------
DATASET_SQL_SOURCES: Dict[str, Dict] = {
    # direct .sql (already working)
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

    # TGZ bundle from Google Drive (MAS/IMDB/YELP)
    "academic": {"type": "bundle", "bundle": "sqlizer", "key": "academic", "out_name": "academic.sql"},
    "imdb": {"type": "bundle", "bundle": "sqlizer", "key": "imdb", "out_name": "imdb.sql"},
    "yelp": {"type": "bundle", "bundle": "sqlizer", "key": "yelp", "out_name": "yelp.sql"},
}

# If user doesn't specify, these are the usual datasets
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


# -----------------------------
# Google Drive downloader (robust)
# -----------------------------
def _extract_gdrive_file_id(url: str) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    fid = (qs.get("id") or [None])[0]
    if fid:
        return fid

    m = re.search(r"/file/d/([A-Za-z0-9_-]+)", url)
    if m:
        return m.group(1)

    raise ValueError("Could not extract Google Drive file id from URL.")


def _save_stream(resp: requests.Response, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Unique temp file to avoid Windows locks / parallel runs
    tmp = out_path.with_suffix(out_path.suffix + f".part.{os.getpid()}.{int(time.time())}")

    try:
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        # Atomic replace
        tmp.replace(out_path)

    finally:
        # Best-effort cleanup (in case replace didn't happen)
        for _ in range(5):
            try:
                if tmp.exists():
                    tmp.unlink()
                break
            except PermissionError:
                time.sleep(0.3)


def _looks_like_html(resp: requests.Response) -> bool:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    return "text/html" in ctype


def _extract_confirm_token_from_html(html: str) -> Optional[str]:
    # confirm=TOKEN in links
    m = re.search(r"[?&]confirm=([0-9A-Za-z_-]+)", html)
    if m:
        return m.group(1)

    # hidden form: name="confirm" value="TOKEN"
    m = re.search(r'name="confirm"\s+value="([^"]+)"', html)
    if m:
        return m.group(1)

    return None


def download_google_drive(url: str, out_path: Path, force: bool = False) -> Path:
    """
    Download from Google Drive handling the "can't scan for viruses" confirm page.
    Always returns a Path on success or raises on failure.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and not force:
        print(f"📦 Using cached: {out_path}")
        return out_path

    file_id = _extract_gdrive_file_id(url)
    print(f"⬇️  Downloading (gdrive): id={file_id}")

    session = requests.Session()

    # We always use the /uc endpoint to simplify the flow.
    base = "https://drive.google.com/uc"
    params = {"export": "download", "id": file_id}

    # 1) First request
    r1 = session.get(base, params=params, stream=True)
    r1.raise_for_status()

    # If it is the file directly (not HTML) -> save
    if not _looks_like_html(r1):
        _save_stream(r1, out_path)
        print(f"✅ Saved: {out_path}")
        return out_path

    # 2) HTML confirmation page -> need token
    html = r1.text

    # A) token in HTML
    token = _extract_confirm_token_from_html(html)

    # B) sometimes token is in cookies: download_warning*
    if token is None:
        for k, v in session.cookies.items():
            if k.startswith("download_warning") and v:
                token = v
                break

    if token is None:
        raise RuntimeError(
            "Google Drive confirmation token not found.\n"
            "Make sure the file is shared as 'Anyone with the link'.\n"
            "If the link downloads in browser but not here, the HTML pattern might differ."
        )

    # 3) Second request with confirm token
    params["confirm"] = token
    r2 = session.get(base, params=params, stream=True)
    r2.raise_for_status()

    # If still HTML -> permissions / not public / needs sign-in
    if _looks_like_html(r2):
        raise RuntimeError(
            "Google Drive returned HTML again (likely permissions / needs sign-in).\n"
            "Ensure sharing is set to 'Anyone with the link'."
        )

    _save_stream(r2, out_path)
    print(f"✅ Saved: {out_path}")
    return out_path


def download_file(url: str, out_path: Path, force: bool = False) -> Path:
    """Generic downloader. Uses Google Drive handler when needed."""
    if "drive.google.com" in url or "drive.usercontent.google.com" in url:
        return download_google_drive(url, out_path, force=force)

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


def extract_tgz(tgz_path: Path, out_dir: Path, force: bool = False) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    marker = out_dir / ".extracted.ok"

    if marker.exists() and not force:
        print(f"📦 Using cached extracted bundle: {out_dir}")
        return out_dir

    # clean dir contents
    if out_dir.exists():
        for item in list(out_dir.iterdir()):
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)

    print(f"🗜️  Extracting TGZ: {tgz_path.name} -> {out_dir}")
    with tarfile.open(tgz_path, "r:gz") as tar:
        tar.extractall(out_dir)

    marker.write_text("ok", encoding="utf-8")
    print(f"✅ Extracted: {out_dir}")
    return out_dir


def find_member_path(extracted_root: Path, member_name: str) -> Path:
    """Find member in extracted tree, even if in subfolder."""
    direct = extracted_root / member_name
    if direct.exists():
        return direct

    matches = list(extracted_root.rglob(member_name))
    if not matches:
        raise FileNotFoundError(f"Member '{member_name}' not found under: {extracted_root}")
    if len(matches) > 1:
        print(f"⚠️  Multiple matches for {member_name}. Using: {matches[0]}")
    return matches[0]


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

    if typ == "bundle":
        bundle_name = src["bundle"]
        key = src.get("key", dataset)
        out_name = src.get("out_name", f"{dataset}.sql")

        if bundle_name not in SQL_BUNDLES:
            raise KeyError(f"Bundle '{bundle_name}' not found in SQL_BUNDLES. Available: {list(SQL_BUNDLES.keys())}")

        bundle = SQL_BUNDLES[bundle_name]
        if bundle.get("type") != "tgz":
            raise ValueError(f"Unsupported bundle type: {bundle.get('type')}")
        # 1) use local tgz if already present
        tgz_target = ARCHIVES_DIR / bundle.get("archive_name", f"{bundle_name}.tgz")
        if tgz_target.exists() and not force_download:
            print(f"📦 Using local cached bundle: {tgz_target}")
            tgz_path = tgz_target
        else:
            tgz_path = download_file(bundle["url"], tgz_target, force=force_download)

        
        # 2) extract
        extracted_root = extract_tgz(
            tgz_path,
            bundle.get("extract_dir", EXTRACTED_DIR / bundle_name),
            force=force_download,
        )

        # 3) locate sql in extracted tree
        member_sql_name = bundle["sql_members"][key]
        member_sql_path = find_member_path(extracted_root, member_sql_name)

        # 4) stage into canonical cache folder
        final_sql_path = CACHE_DIR / out_name
        final_sql_path.parent.mkdir(parents=True, exist_ok=True)

        if (not final_sql_path.exists()) or force_download:
            print(f"📄 Staging SQL: {member_sql_path.name} -> {final_sql_path}")
            shutil.copyfile(member_sql_path, final_sql_path)
        else:
            print(f"📦 Using cached staged SQL: {final_sql_path}")

        # 5) stage questions (optional but desired)
        QUESTIONS_DIR.mkdir(parents=True, exist_ok=True)
        q_map = bundle.get("questions_members", {})
        if key in q_map:
            q_member_name = q_map[key]
            q_member_path = find_member_path(extracted_root, q_member_name)
            q_out = QUESTIONS_DIR / f"{dataset}.questions.txt"

            if (not q_out.exists()) or force_download:
                print(f"📝 Staging Questions: {q_member_path.name} -> {q_out}")
                shutil.copyfile(q_member_path, q_out)
            else:
                print(f"📦 Using cached questions: {q_out}")

        return final_sql_path

    raise ValueError(f"Unknown SQL source type: {typ}")


def docker_mysql_exec(db_type: str, creds: DbCreds, sql: str) -> None:
    """Execute a one-liner SQL command inside the container."""
    container = CONTAINERS[db_type]
    client = "mysql" if db_type == "mysql" else "mariadb"
    cmd = [
        "docker", "exec", "-i", container,
        client, "-uroot", f"-p{creds.root_password}",
        "-e", sql
    ]
    run(cmd)


def docker_mysql_import_file(db_type: str, creds: DbCreds, dataset_db: str, sql_file: Path) -> None:
    """Import a .sql file into a specific database using docker exec + stdin."""
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
    """Dump schema-only using mysqldump / mariadb-dump."""
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

    parser = argparse.ArgumentParser(
        description="Download SQL dumps (if available), import into MySQL/MariaDB, and extract schema snapshots."
    )
    parser.add_argument("--datasets", nargs="+", default=DEFAULT_DATASETS,
                        help="Dataset DBs to import (default: academic imdb yelp advising atis)")
    parser.add_argument("--only", choices=["mysql", "mariadb"], default=None,
                        help="Only run for one RDBMS")
    parser.add_argument("--reset-db", action="store_true",
                        help="Drop dataset DBs before importing")
    parser.add_argument("--force-download", action="store_true",
                        help="Redownload SQL assets even if cached")
    parser.add_argument("--no-schema-dump", action="store_true",
                        help="Skip schema snapshot extraction")
    args = parser.parse_args()

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
            except subprocess.CalledProcessError:
                print(f"❌ Import failed for {db_type}:{dataset}.")
                print("Tip: check container logs and SQL syntax compatibility.")
                return 1

            if not args.no_schema_dump:
                extract_schema_snapshot(db_type, creds, dataset)

    print("\n✅ Done.")
    print(f"📝 Questions saved under: {QUESTIONS_DIR} (if bundle contains them)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

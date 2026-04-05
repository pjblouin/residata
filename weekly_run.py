# weekly_run.py — End-to-end weekly REIT scrape + split + push + rebuild Excel
#
# Runs automatically via Windows Task Scheduler every Saturday at 11 PM.
# Also safe to run manually anytime: py weekly_run.py
#
# Pipeline:
#   1. Run all scrapers (MAA, CPT, EQR, AVB, UDR, ESS, INVH)
#   2. Split any CSV > 4,000 rows or > 1.5 MB into _part1 / _part2
#   3. Git add + commit + push to GitHub
#   4. Rebuild Excel workbook via build_excel.py (pulls from GitHub)
#
# Logs to: logs/weekly_YYYY-MM-DD.log

import os
import sys
import csv
import logging
import subprocess
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"
OUTPUT_DIR = BASE_DIR / "output"

MAX_ROWS = 4000
MAX_BYTES = 1_500_000  # 1.5 MB

today = date.today().isoformat()

LOG_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / f"weekly_{today}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def run_scrapers():
    """Step 1: Run main.py to scrape all REITs."""
    logger.info("=" * 60)
    logger.info("  STEP 1: Running scrapers")
    logger.info("=" * 60)
    result = subprocess.run(
        [sys.executable, str(BASE_DIR / "main.py")],
        cwd=str(BASE_DIR),
        capture_output=False,
        timeout=43200,  # 12-hour timeout
    )
    if result.returncode != 0:
        logger.error(f"main.py exited with code {result.returncode}")
    else:
        logger.info("Scrapers completed successfully.")
    return result.returncode


def split_large_csvs():
    """Step 2: Split any CSV over MAX_ROWS or MAX_BYTES into _part1/_part2."""
    logger.info("=" * 60)
    logger.info("  STEP 2: Splitting large CSVs")
    logger.info("=" * 60)

    for csv_path in sorted(RAW_DIR.glob(f"*_raw_{today}.csv")):
        # Skip already-split files
        if "_part1" in csv_path.name or "_part2" in csv_path.name:
            continue

        file_size = csv_path.stat().st_size
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        row_count = len(rows) - 1  # exclude header
        needs_split = row_count > MAX_ROWS or file_size > MAX_BYTES

        if not needs_split:
            logger.info(f"  {csv_path.name}: {row_count:,} rows, {file_size/1e6:.1f} MB — OK")
            continue

        logger.info(f"  {csv_path.name}: {row_count:,} rows, {file_size/1e6:.1f} MB — SPLITTING")

        header = rows[0]
        data_rows = rows[1:]
        mid = len(data_rows) // 2

        base = csv_path.stem  # e.g. maa_raw_2026-04-04
        for suffix, chunk in [("_part1", data_rows[:mid]), ("_part2", data_rows[mid:])]:
            out_path = csv_path.parent / f"{base}{suffix}.csv"
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(chunk)
            logger.info(f"    -> {out_path.name}: {len(chunk):,} rows")

        # Remove the original unsplit file
        csv_path.unlink()
        logger.info(f"    Removed original {csv_path.name}")


def git_push():
    """Step 3: Git add, commit, push new CSVs to GitHub."""
    logger.info("=" * 60)
    logger.info("  STEP 3: Pushing to GitHub")
    logger.info("=" * 60)

    def git(*args):
        result = subprocess.run(
            ["git"] + list(args),
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.stdout.strip():
            logger.info(f"  git {' '.join(args)}: {result.stdout.strip()[:200]}")
        if result.stderr.strip():
            logger.info(f"  stderr: {result.stderr.strip()[:200]}")
        return result

    git("add", "data/raw/", "data/registry/")

    status = git("status", "--porcelain")
    if not status.stdout.strip():
        logger.info("  No changes to commit.")
        return

    git("commit", "-m", f"Weekly scrape {today}")
    result = git("push")
    if "-> main" in (result.stderr + result.stdout):
        logger.info("  Push successful.")
    else:
        logger.warning("  Push may have failed — check stderr above.")


def rebuild_excel():
    """Step 4: Rebuild the Excel workbook from GitHub data."""
    logger.info("=" * 60)
    logger.info("  STEP 4: Rebuilding Excel workbook")
    logger.info("=" * 60)
    result = subprocess.run(
        [sys.executable, str(BASE_DIR / "build_excel.py")],
        cwd=str(BASE_DIR),
        capture_output=False,
        timeout=600,
    )
    if result.returncode != 0:
        logger.error(f"build_excel.py exited with code {result.returncode}")
    else:
        logger.info("Excel workbook rebuilt successfully.")


def main():
    logger.info(f"Weekly REIT pipeline started — {today}")
    logger.info(f"Base directory: {BASE_DIR}")
    logger.info("")

    # Step 1: Scrape
    rc = run_scrapers()
    if rc != 0:
        logger.warning("Scrapers had errors but continuing with available data...")

    # Step 2: Split large CSVs
    split_large_csvs()

    # Step 3: Push to GitHub
    git_push()

    # Step 4: Rebuild Excel
    rebuild_excel()

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  PIPELINE COMPLETE — {today}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

"""
REIT Rental Scraper — main entry point.

Usage:
  py main.py                      # scrape all configured REITs
  py main.py --reit MAA           # scrape one REIT
  py main.py --reit MAA --limit 5 # test: first 5 communities only
  py main.py --no-save            # run without writing files

Output:
  data/raw/maa_raw_YYYY-MM-DD.csv   — per-REIT raw CSVs
  data/registry/unit_registry.csv   — longitudinal unit tracker
"""

import argparse
import logging
import os
import sys
from datetime import date

import pandas as pd

from config import BASE_DIR, DATA_DIR, RAW_DIR, REITS, CUMULATIVE_EXCEL
from storage.unit_tracker import load_registry, run_tracker
from storage.excel_writer import write_excel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Scraper registry ───────────────────────────────────────────────────────────
# Maps REIT ticker → scrape function.
# Phase 2 scrapers will be added here as they are built.

def _load_scrapers() -> dict:
    scrapers = {}
    try:
        from scrapers.maa import scrape_maa
        scrapers["MAA"] = scrape_maa
    except ImportError as e:
        logger.warning(f"Could not load MAA scraper: {e}")
    try:
        from scrapers.cpt import scrape_cpt
        scrapers["CPT"] = scrape_cpt
    except ImportError as e:
        logger.warning(f"Could not load CPT scraper: {e}")
    try:
        from scrapers.eqr import scrape_eqr
        scrapers["EQR"] = scrape_eqr
    except ImportError as e:
        logger.warning(f"Could not load EQR scraper: {e}")
    try:
        from scrapers.avb import scrape_avb
        scrapers["AVB"] = scrape_avb
    except ImportError as e:
        logger.warning(f"Could not load AVB scraper: {e}")
    try:
        from scrapers.udr import scrape_udr
        scrapers["UDR"] = scrape_udr
    except ImportError as e:
        logger.warning(f"Could not load UDR scraper: {e}")
    try:
        from scrapers.ess import scrape_ess
        scrapers["ESS"] = scrape_ess
    except ImportError as e:
        logger.warning(f"Could not load ESS scraper: {e}")
    try:
        from scrapers.invh import scrape_invh
        scrapers["INVH"] = scrape_invh
    except ImportError as e:
        logger.warning(f"Could not load INVH scraper: {e}")
    try:
        from scrapers.amh import scrape_amh
        scrapers["AMH"] = scrape_amh
    except ImportError as e:
        logger.warning(f"Could not load AMH scraper: {e}")
    return scrapers


# ── Main ───────────────────────────────────────────────────────────────────────

def run(reit: str | None, limit: int | None, save: bool) -> pd.DataFrame:
    scrapers = _load_scrapers()
    targets = [reit.upper()] if reit else list(scrapers.keys())

    all_frames = []
    failed = []
    for ticker in targets:
        if ticker not in scrapers:
            logger.warning(f"No scraper available for {ticker} — skipping.")
            continue

        logger.info(f"{'='*60}")
        logger.info(f"  Scraping {ticker}")
        logger.info(f"{'='*60}")

        scrape_fn = scrapers[ticker]

        # ── Isolated execution with retry ──────────────────────────
        # Each scraper runs inside try/except so one failure never
        # kills the remaining REITs.  Retries once on transient errors
        # (network timeouts, browser crashes, etc.).
        df = pd.DataFrame()
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                df = scrape_fn(limit=limit)
                break  # success
            except Exception as e:
                logger.error(f"{ticker} attempt {attempt}/{max_attempts} failed: "
                             f"{type(e).__name__}: {e}")
                if attempt < max_attempts:
                    logger.info(f"  Retrying {ticker}...")
                else:
                    logger.error(f"  {ticker} FAILED after {max_attempts} attempts — skipping.")
                    failed.append(ticker)

        if df.empty:
            if ticker not in failed:
                logger.warning(f"{ticker}: 0 rows returned — skipping tracker/save.")
            continue

        # Run longitudinal tracker — updates first_seen / last_seen
        try:
            df = run_tracker(df, data_dir=DATA_DIR, scrape_date=date.today())
        except Exception as e:
            logger.error(f"{ticker} tracker failed: {type(e).__name__}: {e}")
            logger.info(f"  Saving raw data without tracker merge.")

        logger.info(f"{ticker}: {len(df):,} rows after tracker merge.")

        if save:
            os.makedirs(RAW_DIR, exist_ok=True)
            out_path = os.path.join(RAW_DIR, f"{ticker.lower()}_raw_{date.today().isoformat()}.csv")
            df.to_csv(out_path, index=False)
            logger.info(f"  Saved → {out_path}")

        all_frames.append(df)

    if failed:
        logger.warning(f"\n{'!'*60}")
        logger.warning(f"  FAILED REITs: {', '.join(failed)}")
        logger.warning(f"{'!'*60}")

    if not all_frames:
        logger.error("No data collected.")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    logger.info(f"\nTotal rows across all REITs: {len(combined):,}")
    logger.info(f"Successful: {len(all_frames)} REITs | Failed: {len(failed)}")

    if save:
        registry_df = load_registry(DATA_DIR)
        write_excel(combined, registry_df, CUMULATIVE_EXCEL)

    # Exit code 0 if we got at least some data, 1 only if total failure
    return combined


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="REIT Rental Scraper")
    parser.add_argument("--reit",    type=str,  default=None,
                        help="Single REIT ticker to scrape (e.g. MAA)")
    parser.add_argument("--limit",   type=int,  default=None,
                        help="Limit to N communities per REIT (testing)")
    parser.add_argument("--no-save", action="store_true",
                        help="Skip writing output files")
    args = parser.parse_args()

    df = run(
        reit=args.reit,
        limit=args.limit,
        save=not args.no_save,
    )

    if not df.empty:
        print("\n--- Sample output (first 5 rows) ---")
        cols = ["scrape_date", "reit", "community", "market", "unit_id",
                "beds", "sqft", "rent", "concession_hardness",
                "concession_type", "concession_value", "effective_monthly_rent",
                "first_seen", "last_seen"]
        print(df[cols].head(5).to_string(index=False))
        print(f"\nShape: {df.shape}")
        if "has_concession" in df.columns:
            print(f"\nConcession rate: {df['has_concession'].mean():.1%}")

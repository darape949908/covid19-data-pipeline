"""
main.py
-------
Pipeline orchestrator for the COVID-19 Data Engineering project.

Run this file to execute the full pipeline:
  1. Ingest  → download and load raw OWID CSV
  2. Process → clean, transform, feature-engineer
  3. Store   → load into SQLite data warehouse
  4. Analyse → compute KPIs and export CSV summaries
  5. (Optional) Visualise → export static HTML charts

Usage:
    python main.py                     # run full pipeline
    python main.py --skip-download     # skip download if CSV already exists
    python main.py --export-charts     # also export static chart HTMLs

Author : Chandra Kanth Darapeneni
Date   : November 2023
"""

import sys
import os
import time
import argparse
from datetime import datetime
from colorama import init, Fore, Style

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from src.ingestion   import ingest, download_dataset
from src.processing  import process
from src.database    import load_to_database
from src.analysis    import run_analysis

init(autoreset=True)   # colorama — makes coloured output work on Windows too


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def print_banner():
    banner = f"""
{Fore.CYAN}╔══════════════════════════════════════════════════════════╗
║   COVID-19 GLOBAL DATA ENGINEERING PIPELINE              ║
║   Author  : Chandra Kanth Darapeneni                     ║
║   Personal Portfolio Project | 2023                      ║
╚══════════════════════════════════════════════════════════╝{Style.RESET_ALL}
"""
    print(banner)


def print_step_result(step: str, elapsed: float, status: str = "OK"):
    color = Fore.GREEN if status == "OK" else Fore.RED
    print(f"\n{color}  [{status}] {step} — {elapsed:.1f}s{Style.RESET_ALL}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="COVID-19 Data Engineering Pipeline"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download if raw CSV already exists"
    )
    parser.add_argument(
        "--export-charts",
        action="store_true",
        help="Export static HTML charts after analysis"
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force re-download even if CSV exists"
    )
    return parser.parse_args()


# ------------------------------------------------------------------
# Pipeline steps
# ------------------------------------------------------------------

def run_pipeline(skip_download: bool = False,
                 export_charts : bool = False,
                 force_download: bool = False) -> None:
    """
    Execute the full data engineering pipeline.
    """
    pipeline_start = time.time()
    print_banner()
    print(f"  Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # ---- Step 1: Ingestion ----------------------------------------
    t0 = time.time()
    try:
        if not skip_download or force_download:
            download_dataset(force_download=force_download)
        raw_df = ingest()
        print_step_result("Ingestion", time.time() - t0)
    except Exception as e:
        print(f"{Fore.RED}  [FAILED] Ingestion: {e}{Style.RESET_ALL}")
        sys.exit(1)

    # ---- Step 2: Processing ----------------------------------------
    t0 = time.time()
    try:
        processed = process(raw_df)
        print_step_result("Processing", time.time() - t0)
    except Exception as e:
        print(f"{Fore.RED}  [FAILED] Processing: {e}{Style.RESET_ALL}")
        sys.exit(1)

    # ---- Step 3: Database ------------------------------------------
    t0 = time.time()
    try:
        load_to_database(processed)
        print_step_result("Database Loading", time.time() - t0)
    except Exception as e:
        print(f"{Fore.RED}  [FAILED] Database Loading: {e}{Style.RESET_ALL}")
        sys.exit(1)

    # ---- Step 4: Analysis ------------------------------------------
    t0 = time.time()
    try:
        results = run_analysis()
        print_step_result("Analysis", time.time() - t0)
    except Exception as e:
        print(f"{Fore.RED}  [FAILED] Analysis: {e}{Style.RESET_ALL}")
        sys.exit(1)

    # ---- Step 5: Charts (optional) ---------------------------------
    if export_charts:
        t0 = time.time()
        try:
            from src.visualizations import export_all_charts
            export_all_charts()
            print_step_result("Chart Export", time.time() - t0)
        except Exception as e:
            print(f"{Fore.YELLOW}  [WARN] Chart export failed: {e}{Style.RESET_ALL}")
            print(f"  (This is non-critical — try installing kaleido for PNG export)")

    # ---- Done ------------------------------------------------------
    total = time.time() - pipeline_start
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"  ✓ PIPELINE COMPLETE — total time: {total:.1f}s")
    print(f"  Database  : {config.DB_PATH}")
    print(f"  Outputs   : {config.OUTPUT_DIR}")
    print(f"  Dashboard : run  python dashboard/app.py  then open")
    print(f"              http://{config.DASH_HOST}:{config.DASH_PORT}")
    print(f"{'='*60}{Style.RESET_ALL}\n")


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    run_pipeline(
        skip_download = args.skip_download,
        export_charts = args.export_charts,
        force_download = args.force_download,
    )

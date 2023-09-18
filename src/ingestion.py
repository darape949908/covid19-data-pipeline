"""
src/ingestion.py
----------------
Handles downloading and loading the raw COVID-19 dataset.

The Our World in Data (OWID) dataset is a well-maintained CSV that
consolidates WHO data, national health ministry reports, and vaccination
records from across the globe. It's updated daily and is free to use.

I first tried using the disease.sh REST API (https://disease.sh/) because
I thought a live API would be cleaner than downloading a CSV, but it was
missing most of the historical vaccination data before mid-2021. Switched
to the OWID single-file CSV which has everything in one place going back
to Jan 2020.

Author : Chandra Kanth Darapeneni
Date   : September 2023
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm

# Add project root to path so we can import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def download_dataset(url: str = config.OWID_COVID_URL,
                     destination: str = config.RAW_CSV_PATH,
                     force_download: bool = False) -> str:
    """
    Download the OWID COVID CSV to the local data/ directory.

    If the file already exists, skip the download unless force_download=True.
    Uses a streaming download with a progress bar since the file is ~60 MB.

    Parameters
    ----------
    url : str
        Direct URL to the OWID CSV.
    destination : str
        Local path where the file will be saved.
    force_download : bool
        Re-download even if file already exists.

    Returns
    -------
    str
        Path to the downloaded (or existing) file.
    """
    if os.path.exists(destination) and not force_download:
        size_mb = os.path.getsize(destination) / (1024 * 1024)
        print(f"[ingestion] Dataset already exists ({size_mb:.1f} MB). "
              f"Skipping download. Use force_download=True to re-fetch.")
        return destination

    print(f"[ingestion] Downloading dataset from:\n  {url}")
    print(f"[ingestion] Saving to: {destination}")

    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        block_size = 8192  # 8 KB chunks

        with open(destination, "wb") as f, tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="Downloading",
            colour="green",
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:
                    f.write(chunk)
                    progress_bar.update(len(chunk))

        size_mb = os.path.getsize(destination) / (1024 * 1024)
        print(f"[ingestion] ✓ Download complete — {size_mb:.1f} MB saved.")
        return destination

    except requests.exceptions.ConnectionError:
        print("[ingestion] ✗ Could not connect to the internet.")
        print("[ingestion]   Looking for a cached version in data/ ...")
        if os.path.exists(destination):
            print("[ingestion]   Found cached file. Using it.")
            return destination
        raise RuntimeError("No internet connection and no cached dataset found.")

    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"HTTP error while downloading dataset: {e}")


def load_raw_csv(path: str = config.RAW_CSV_PATH) -> pd.DataFrame:
    """
    Load the raw OWID CSV into a Pandas DataFrame.

    Basic dtype hints are applied at load time so date parsing
    doesn't explode later.

    Parameters
    ----------
    path : str
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        Raw, unmodified data.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Raw CSV not found at '{path}'.\n"
            "Run download_dataset() first, or place the file manually."
        )

    print(f"[ingestion] Loading CSV: {path}")
    df = pd.read_csv(
        path,
        parse_dates=["date"],
        low_memory=False,     # suppresses mixed-type warning on large CSVs
    )

    print(f"[ingestion] ✓ Loaded {len(df):,} rows × {len(df.columns)} columns.")
    print(f"[ingestion]   Date range: {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"[ingestion]   Countries/locations: {df['location'].nunique()}")

    return df


def get_column_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Quick overview of column completeness — useful during exploration.

    Returns a DataFrame showing each column, its dtype,
    number of nulls, and % completeness.
    """
    summary = pd.DataFrame({
        "dtype"       : df.dtypes,
        "null_count"  : df.isnull().sum(),
        "null_pct"    : (df.isnull().sum() / len(df) * 100).round(2),
        "sample_value": df.apply(lambda col: col.dropna().iloc[0] if col.dropna().any() else None),
    })
    summary = summary.sort_values("null_pct", ascending=False)
    return summary


def ingest() -> pd.DataFrame:
    """
    Full ingestion step: download (if needed) → load → return raw DataFrame.
    This is the function called by main.py.
    """
    print("\n" + "="*60)
    print("  STEP 1 — DATA INGESTION")
    print("="*60)

    download_dataset()
    df = load_raw_csv()

    # Log a brief summary
    print("\n[ingestion] Column null summary (top 10 most incomplete):")
    summary = get_column_summary(df).head(10)
    print(summary[["dtype", "null_count", "null_pct"]].to_string())

    return df


# Quick test — run this file directly to check ingestion works
if __name__ == "__main__":
    data = ingest()
    print("\nFirst 5 rows:")
    print(data.head())

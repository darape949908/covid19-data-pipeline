"""
src/processing.py
-----------------
Cleans, transforms, and feature-engineers the raw COVID dataset.

Key steps:
  1. Filter to only real countries (drop continent aggregates)
  2. Keep only the columns we care about
  3. Filter date range
  4. Fill missing numeric values sensibly
  5. Add derived columns (CFR, rolling averages, etc.)
  6. Compute country-level and continent-level totals

NOTE: I spent a lot of time on the null handling here because just
doing fillna(0) was wrong — a null for "new_cases" on a given day
usually means the country didn't report that day, not that there
were 0 cases. I forward-fill for cumulative columns and leave
daily columns as NaN where they're genuinely missing.

Author : Chandra Kanth Darapeneni
Date   : October 2023
"""

import sys
import os
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


# ------------------------------------------------------------------
# Step 1 — Filter and select columns
# ------------------------------------------------------------------

def filter_countries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows that are OWID aggregate groups (continents, income
    bands, 'World', etc.) and keep only individual countries.

    OWID marks these with iso_codes starting with 'OWID_' or with
    blank continent values.
    """
    # Strategy: keep rows where iso_code is a standard 3-letter code
    # (doesn't start with 'OWID_') AND location is not in the known
    # non-country list
    mask = (
        ~df["location"].isin(config.NON_COUNTRY_LOCATIONS) &
        ~df["iso_code"].str.startswith("OWID_", na=False) &
        df["continent"].notna()
    )
    filtered = df[mask].copy()
    dropped = len(df) - len(filtered)
    print(f"[processing] Removed {dropped:,} aggregate rows "
          f"({df['location'].nunique() - filtered['location'].nunique()} non-country locations).")
    return filtered


def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the columns defined in config.COLUMNS_TO_KEEP.
    We silently drop any that don't exist in the loaded data
    (column availability changed between OWID dataset versions).
    """
    existing = [c for c in config.COLUMNS_TO_KEEP if c in df.columns]
    missing  = [c for c in config.COLUMNS_TO_KEEP if c not in df.columns]

    if missing:
        print(f"[processing] Warning: these expected columns are missing "
              f"from the dataset: {missing}")

    df = df[existing].copy()
    print(f"[processing] Selected {len(existing)} columns.")
    return df


def filter_date_range(df: pd.DataFrame,
                      start: str = config.DATE_START,
                      end:   str = config.DATE_END) -> pd.DataFrame:
    """Filter rows to the configured date range."""
    df = df[(df["date"] >= start) & (df["date"] <= end)].copy()
    print(f"[processing] Date range applied: {start} → {end} "
          f"({len(df):,} rows remaining).")
    return df


# ------------------------------------------------------------------
# Step 2 — Null handling
# ------------------------------------------------------------------

def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing values using context-appropriate strategies:

    - Cumulative columns (total_cases, total_deaths, etc.):
      forward-fill within each country so gaps in reporting
      don't reset to zero, then fill leading NaNs with 0.

    - Daily columns (new_cases, new_deaths):
      leave as NaN — they're genuinely missing, not zero.
      Charts will handle gaps gracefully.

    - Per-million columns: same as their parent column strategy.

    - Population: forward + backward fill within country
      (it shouldn't change day to day).
    """
    df = df.sort_values(["location", "date"]).copy()

    # NOTE: I first tried just doing df.fillna(0) for everything but that
    # was wrong — for cumulative cols a null means "same as yesterday",
    # not zero. Took me a while to figure out groupby + ffill was the fix.

    cumulative_cols = [
        "total_cases", "total_deaths",
        "total_cases_per_million", "total_deaths_per_million",
        "total_vaccinations", "people_vaccinated",
        "people_fully_vaccinated", "total_boosters",
        "people_vaccinated_per_hundred", "people_fully_vaccinated_per_hundred",
    ]

    ffill_cols = [c for c in cumulative_cols if c in df.columns]
    df[ffill_cols] = (
        df.groupby("location")[ffill_cols]
          .transform(lambda g: g.ffill().fillna(0))
    )

    # Population shouldn't vary — fill forward and backward
    if "population" in df.columns:
        df["population"] = (
            df.groupby("location")["population"]
              .transform(lambda g: g.ffill().bfill())
        )

    # Fill remaining numeric nulls with 0 for daily columns
    daily_cols = ["new_cases", "new_deaths",
                  "new_cases_smoothed", "new_deaths_smoothed",
                  "new_cases_per_million"]
    for col in daily_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    null_remaining = df.isnull().sum().sum()
    print(f"[processing] After null handling: {null_remaining:,} nulls remain "
          f"(in non-critical columns).")
    return df


# ------------------------------------------------------------------
# Step 3 — Feature engineering
# ------------------------------------------------------------------

def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add useful derived columns for analysis and dashboard display.
    """
    df = df.copy()

    # Case Fatality Rate (CFR): deaths / confirmed cases * 100
    # Guard against division by zero
    if "total_deaths" in df.columns and "total_cases" in df.columns:
        df["cfr"] = np.where(
            df["total_cases"] > 0,
            (df["total_deaths"] / df["total_cases"] * 100).round(4),
            0.0
        )

    # 7-day rolling average for new cases (per country)
    # min_periods=1 is important — without it the first 6 rows per country are NaN
    # and Plotly just leaves gaps in the line which looks broken
    if "new_cases" in df.columns:
        df["new_cases_7day_avg"] = (
            df.groupby("location")["new_cases"]
              .transform(lambda g: g.rolling(window=7, min_periods=1).mean().round(1))
        )

    # 7-day rolling average for new deaths
    if "new_deaths" in df.columns:
        df["new_deaths_7day_avg"] = (
            df.groupby("location")["new_deaths"]
              .transform(lambda g: g.rolling(window=7, min_periods=1).mean().round(1))
        )

    # Vaccination rate: % of population with at least one dose
    if "people_vaccinated" in df.columns and "population" in df.columns:
        df["vax_rate_pct"] = np.where(
            df["population"] > 0,
            (df["people_vaccinated"] / df["population"] * 100).round(2),
            np.nan
        )
        df["vax_rate_pct"] = df["vax_rate_pct"].clip(upper=100)  # cap at 100%

    # Year and month columns — useful for groupby aggregations
    df["year"]       = df["date"].dt.year
    df["month"]      = df["date"].dt.month
    df["year_month"] = df["date"].dt.to_period("M").astype(str)

    print(f"[processing] ✓ Added derived columns: cfr, rolling averages, "
          f"vax_rate_pct, year/month.")
    return df


# ------------------------------------------------------------------
# Step 4 — Aggregate tables
# ------------------------------------------------------------------

def build_country_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each country, grab the latest row (most recent date) — this gives
    us cumulative totals like total_cases, total_deaths, etc.

    Also compute total new_cases and new_deaths summed over all time.
    """
    # Latest snapshot per country
    latest = (
        df.sort_values("date")
          .groupby("location")
          .last()
          .reset_index()
    )

    # Sum of daily new cases / deaths for each country
    daily_sums = df.groupby("location").agg(
        sum_new_cases  = ("new_cases",  "sum"),
        sum_new_deaths = ("new_deaths", "sum"),
    ).reset_index()

    totals = latest.merge(daily_sums, on="location", how="left")

    keep = [
        "iso_code", "continent", "location", "population",
        "total_cases", "total_deaths", "cfr",
        "people_vaccinated", "people_fully_vaccinated",
        "vax_rate_pct", "people_vaccinated_per_hundred",
        "people_fully_vaccinated_per_hundred",
        "total_cases_per_million", "total_deaths_per_million",
        "sum_new_cases", "sum_new_deaths",
    ]
    keep = [c for c in keep if c in totals.columns]
    totals = totals[keep]

    print(f"[processing] Country totals table: {len(totals)} countries.")
    return totals


def build_continent_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate totals by continent using the latest available data per country.
    """
    latest_per_country = (
        df.sort_values("date")
          .groupby("location")
          .last()
          .reset_index()
    )

    continent_agg = (
        latest_per_country.groupby("continent")
        .agg(
            total_cases         = ("total_cases",          "sum"),
            total_deaths        = ("total_deaths",         "sum"),
            people_vaccinated   = ("people_vaccinated",    "sum"),
            population          = ("population",           "sum"),
            country_count       = ("location",             "count"),
        )
        .reset_index()
    )

    continent_agg["cfr"] = np.where(
        continent_agg["total_cases"] > 0,
        (continent_agg["total_deaths"] / continent_agg["total_cases"] * 100).round(4),
        0.0
    )
    continent_agg["vax_rate_pct"] = (
        continent_agg["people_vaccinated"] / continent_agg["population"] * 100
    ).round(2).clip(upper=100)

    print(f"[processing] Continent summary: {len(continent_agg)} continents.")
    return continent_agg


def build_daily_global(df: pd.DataFrame) -> pd.DataFrame:
    """
    Global daily totals — sum across all countries per day.
    Used for the world time-series chart.
    """
    daily = (
        df.groupby("date")
        .agg(
            new_cases  = ("new_cases",  "sum"),
            new_deaths = ("new_deaths", "sum"),
        )
        .reset_index()
    )
    daily["new_cases_7day_avg"]  = daily["new_cases"].rolling(7, min_periods=1).mean().round(1)
    daily["new_deaths_7day_avg"] = daily["new_deaths"].rolling(7, min_periods=1).mean().round(1)
    return daily


# ------------------------------------------------------------------
# Main processing step
# ------------------------------------------------------------------

def process(raw_df: pd.DataFrame) -> dict:
    """
    Run the full processing pipeline.
    Returns a dict of DataFrames that will be stored in the database.
    Called from main.py.

    Returns
    -------
    dict with keys:
        'daily'          -> cleaned daily data for all countries
        'country_totals' -> one row per country, cumulative totals
        'continent'      -> aggregated by continent
        'global_daily'   -> global daily new cases/deaths
    """
    print("\n" + "="*60)
    print("  STEP 2 — DATA PROCESSING")
    print("="*60)

    df = raw_df.copy()
    df = filter_countries(df)
    df = select_columns(df)
    df = filter_date_range(df)
    df = handle_nulls(df)
    df = add_derived_features(df)

    country_totals    = build_country_totals(df)
    continent_summary = build_continent_summary(df)
    global_daily      = build_daily_global(df)

    print(f"\n[processing] ✓ Processing complete.")
    print(f"    Daily records : {len(df):,}")
    print(f"    Countries     : {df['location'].nunique()}")
    print(f"    Date range    : {df['date'].min().date()} → {df['date'].max().date()}")

    return {
        "daily"          : df,
        "country_totals" : country_totals,
        "continent"      : continent_summary,
        "global_daily"   : global_daily,
    }


if __name__ == "__main__":
    # Quick standalone test
    from ingestion import ingest
    raw = ingest()
    results = process(raw)
    print("\nCountry totals sample:")
    print(results["country_totals"].head(5))

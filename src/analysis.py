"""
src/analysis.py
---------------
Computes KPIs, statistical summaries, and aggregations
that feed into the dashboard and exported reports.

All functions query the SQLite database (not raw DataFrames)
to simulate a real analytical workflow where you'd query a warehouse.

Author : Chandra Kanth Darapeneni
Date   : October 2023
"""

import sys
import os
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from src.database import get_connection, query, get_table


# ------------------------------------------------------------------
# Global KPI Cards
# ------------------------------------------------------------------

def get_global_kpis() -> dict:
    """
    Returns the high-level global numbers shown on dashboard KPI cards.

    Returns
    -------
    dict with keys:
        total_cases, total_deaths, total_vaccinations,
        cfr_global, countries_affected
    """
    conn = get_connection()
    try:
        sql = """
            SELECT
                SUM(total_cases)          AS total_cases,
                SUM(total_deaths)         AS total_deaths,
                SUM(people_vaccinated)    AS people_vaccinated,
                SUM(people_fully_vaccinated) AS people_fully_vaccinated,
                COUNT(*)                  AS countries_affected
            FROM country_totals
            WHERE total_cases > 0
        """
        row = pd.read_sql_query(sql, conn).iloc[0]

        cfr = (row["total_deaths"] / row["total_cases"] * 100) if row["total_cases"] > 0 else 0

        # TODO: add active_cases once I figure out how OWID calculates it
        # (total_cases - total_deaths doesn't seem right, they removed the
        # recovered column from the dataset at some point)

        return {
            "total_cases"             : int(row["total_cases"] or 0),
            "total_deaths"            : int(row["total_deaths"] or 0),
            "people_vaccinated"       : int(row["people_vaccinated"] or 0),
            "people_fully_vaccinated" : int(row["people_fully_vaccinated"] or 0),
            "cfr_global"              : round(cfr, 3),
            "countries_affected"      : int(row["countries_affected"]),
        }
    finally:
        conn.close()


# ------------------------------------------------------------------
# Top Countries
# ------------------------------------------------------------------

def get_top_countries_by_cases(n: int = config.TOP_N_COUNTRIES) -> pd.DataFrame:
    """
    Return top N countries ranked by total confirmed cases.
    """
    sql = f"""
        SELECT
            location,
            continent,
            total_cases,
            total_deaths,
            cfr,
            population,
            total_cases_per_million,
            vax_rate_pct
        FROM country_totals
        WHERE total_cases > 0
          AND location IS NOT NULL
        ORDER BY total_cases DESC
        LIMIT {n}
    """
    return query(sql)


def get_top_countries_by_deaths(n: int = config.TOP_N_COUNTRIES) -> pd.DataFrame:
    """Return top N countries by total deaths."""
    sql = f"""
        SELECT location, continent, total_deaths, total_cases, cfr
        FROM country_totals
        WHERE total_deaths > 0
        ORDER BY total_deaths DESC
        LIMIT {n}
    """
    return query(sql)


def get_top_countries_by_cfr(n: int = 20, min_cases: int = 10000) -> pd.DataFrame:
    """
    Return countries with the highest Case Fatality Rate.
    A minimum case threshold avoids noise from very small countries.
    """
    sql = f"""
        SELECT location, continent, cfr, total_cases, total_deaths
        FROM country_totals
        WHERE total_cases >= {min_cases}
          AND cfr > 0
        ORDER BY cfr DESC
        LIMIT {n}
    """
    return query(sql)


# ------------------------------------------------------------------
# Time Series
# ------------------------------------------------------------------

def get_global_time_series() -> pd.DataFrame:
    """Global daily new cases and deaths over time."""
    sql = """
        SELECT date, new_cases, new_deaths,
               new_cases_7day_avg, new_deaths_7day_avg
        FROM global_daily
        ORDER BY date
    """
    df = query(sql)
    df["date"] = pd.to_datetime(df["date"])
    return df


def get_country_time_series(location: str) -> pd.DataFrame:
    """
    Daily stats for a single country.
    Used by the country-level drilldown chart.
    """
    sql = f"""
        SELECT date, new_cases, new_deaths,
               new_cases_7day_avg, new_deaths_7day_avg,
               total_cases, total_deaths,
               people_vaccinated_per_hundred,
               stringency_index
        FROM daily_stats
        WHERE location = '{location}'
        ORDER BY date
    """
    df = query(sql)
    df["date"] = pd.to_datetime(df["date"])
    return df


def get_monthly_global() -> pd.DataFrame:
    """Aggregate global new cases/deaths by month."""
    sql = """
        SELECT
            year_month,
            SUM(new_cases)  AS monthly_cases,
            SUM(new_deaths) AS monthly_deaths
        FROM daily_stats
        GROUP BY year_month
        ORDER BY year_month
    """
    return query(sql)


# ------------------------------------------------------------------
# Continent Analysis
# ------------------------------------------------------------------

def get_continent_breakdown() -> pd.DataFrame:
    """Continent-level totals."""
    sql = """
        SELECT continent, total_cases, total_deaths,
               people_vaccinated, population,
               cfr, vax_rate_pct, country_count
        FROM continent_summary
        ORDER BY total_cases DESC
    """
    return query(sql)


def get_cases_by_continent_over_time() -> pd.DataFrame:
    """
    Monthly new cases grouped by continent.
    Used for the stacked area chart.
    """
    sql = """
        SELECT
            year_month,
            continent,
            SUM(new_cases) AS monthly_cases
        FROM daily_stats
        WHERE continent IS NOT NULL
        GROUP BY year_month, continent
        ORDER BY year_month, continent
    """
    return query(sql)


# ------------------------------------------------------------------
# Vaccination Analysis
# ------------------------------------------------------------------

def get_vaccination_leaders(n: int = 20) -> pd.DataFrame:
    """
    Top N countries by vaccination rate (% population with 1+ dose).
    """
    sql = f"""
        SELECT
            location,
            continent,
            people_vaccinated_per_hundred,
            people_fully_vaccinated_per_hundred,
            vax_rate_pct,
            population
        FROM country_totals
        WHERE people_vaccinated_per_hundred > 0
          AND population > 1000000   -- exclude micro-states
        ORDER BY people_vaccinated_per_hundred DESC
        LIMIT {n}
    """
    return query(sql)


def get_vaccination_by_continent() -> pd.DataFrame:
    """Average vaccination rate per continent."""
    sql = """
        SELECT continent, AVG(vax_rate_pct) AS avg_vax_rate
        FROM country_totals
        WHERE vax_rate_pct IS NOT NULL
          AND continent IS NOT NULL
        GROUP BY continent
        ORDER BY avg_vax_rate DESC
    """
    return query(sql)


# ------------------------------------------------------------------
# Map Data
# ------------------------------------------------------------------

def get_choropleth_data(metric: str = "total_cases") -> pd.DataFrame:
    """
    Returns country-level data with ISO codes for Plotly choropleth map.

    Parameters
    ----------
    metric : column name to visualise on the map
    """
    valid_metrics = [
        "total_cases", "total_deaths", "cfr",
        "vax_rate_pct", "total_cases_per_million",
        "total_deaths_per_million"
    ]
    if metric not in valid_metrics:
        raise ValueError(f"metric must be one of {valid_metrics}")

    sql = f"""
        SELECT iso_code, location, continent,
               {metric} AS value,
               total_cases, total_deaths, cfr,
               vax_rate_pct, population
        FROM country_totals
        WHERE iso_code IS NOT NULL
          AND {metric} IS NOT NULL
    """
    return query(sql)


# ------------------------------------------------------------------
# Summary export
# ------------------------------------------------------------------

def export_summary_csv(output_dir: str = config.OUTPUT_DIR) -> None:
    """
    Export key analysis tables to CSV for easy sharing / submission.
    """
    os.makedirs(output_dir, exist_ok=True)

    exports = {
        "global_kpis.csv"        : pd.DataFrame([get_global_kpis()]),
        "top20_cases.csv"        : get_top_countries_by_cases(20),
        "top20_deaths.csv"       : get_top_countries_by_deaths(20),
        "continent_summary.csv"  : get_continent_breakdown(),
        "vaccination_leaders.csv": get_vaccination_leaders(20),
    }

    for filename, df in exports.items():
        path = os.path.join(output_dir, filename)
        df.to_csv(path, index=False)
        print(f"[analysis] Exported: {path}")

    print(f"[analysis] ✓ All summaries exported to {output_dir}")


def run_analysis() -> dict:
    """
    Run all analytical queries and return results.
    Called from main.py after database loading.
    """
    print("\n" + "="*60)
    print("  STEP 4 — ANALYSIS & KPI COMPUTATION")
    print("="*60)

    kpis = get_global_kpis()
    print("\n[analysis] Global KPIs:")
    for k, v in kpis.items():
        if isinstance(v, float):
            print(f"    {k:<30} {v:.3f}")
        else:
            print(f"    {k:<30} {v:,}" if isinstance(v, int) else f"    {k:<30} {v}")

    print("\n[analysis] Exporting summary CSVs ...")
    export_summary_csv()

    print("\n[analysis] ✓ Analysis complete.")
    return {"kpis": kpis}


if __name__ == "__main__":
    results = run_analysis()

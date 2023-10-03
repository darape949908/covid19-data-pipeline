"""
src/database.py
---------------
Creates the SQLite schema and loads processed DataFrames
into the local data warehouse.

I chose SQLite for simplicity — it's file-based, no server setup
required, and Pandas integrates with it cleanly via sqlite3.
In a real production pipeline I would use PostgreSQL or BigQuery,
but SQLite is fine for a project of this scale.

The "warehouse" is really just 4 tables:
  1. raw_covid         — full daily data, all countries
  2. daily_stats       — alias for the cleaned daily data
  3. country_totals    — one row per country, cumulative numbers
  4. continent_summary — aggregated by continent

Author : Chandra Kanth Darapeneni
Date   : October 2023
"""

import os
import sys
import sqlite3
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


# ------------------------------------------------------------------
# Connection helper
# ------------------------------------------------------------------

def get_connection(db_path: str = config.DB_PATH) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database and return a connection.
    Also enables WAL mode for slightly better write performance.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# ------------------------------------------------------------------
# Schema creation
# ------------------------------------------------------------------

CREATE_STATEMENTS = {

    "daily_stats": """
        CREATE TABLE IF NOT EXISTS daily_stats (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            iso_code                        TEXT,
            continent                       TEXT,
            location                        TEXT NOT NULL,
            date                            TEXT NOT NULL,
            total_cases                     REAL,
            new_cases                       REAL,
            new_cases_smoothed              REAL,
            total_deaths                    REAL,
            new_deaths                      REAL,
            new_deaths_smoothed             REAL,
            total_cases_per_million         REAL,
            new_cases_per_million           REAL,
            total_deaths_per_million        REAL,
            reproduction_rate               REAL,
            total_vaccinations              REAL,
            people_vaccinated               REAL,
            people_fully_vaccinated         REAL,
            total_boosters                  REAL,
            people_vaccinated_per_hundred   REAL,
            people_fully_vaccinated_per_hundred REAL,
            population                      REAL,
            hospital_patients               REAL,
            icu_patients                    REAL,
            stringency_index                REAL,
            cfr                             REAL,
            new_cases_7day_avg              REAL,
            new_deaths_7day_avg             REAL,
            vax_rate_pct                    REAL,
            year                            INTEGER,
            month                           INTEGER,
            year_month                      TEXT
        );
    """,

    "country_totals": """
        CREATE TABLE IF NOT EXISTS country_totals (
            iso_code                        TEXT PRIMARY KEY,
            continent                       TEXT,
            location                        TEXT NOT NULL,
            population                      REAL,
            total_cases                     REAL,
            total_deaths                    REAL,
            cfr                             REAL,
            people_vaccinated               REAL,
            people_fully_vaccinated         REAL,
            vax_rate_pct                    REAL,
            people_vaccinated_per_hundred   REAL,
            people_fully_vaccinated_per_hundred REAL,
            total_cases_per_million         REAL,
            total_deaths_per_million        REAL,
            sum_new_cases                   REAL,
            sum_new_deaths                  REAL
        );
    """,

    "continent_summary": """
        CREATE TABLE IF NOT EXISTS continent_summary (
            continent           TEXT PRIMARY KEY,
            total_cases         REAL,
            total_deaths        REAL,
            people_vaccinated   REAL,
            population          REAL,
            country_count       INTEGER,
            cfr                 REAL,
            vax_rate_pct        REAL
        );
    """,

    "global_daily": """
        CREATE TABLE IF NOT EXISTS global_daily (
            date                  TEXT PRIMARY KEY,
            new_cases             REAL,
            new_deaths            REAL,
            new_cases_7day_avg    REAL,
            new_deaths_7day_avg   REAL
        );
    """,

    "pipeline_log": """
        CREATE TABLE IF NOT EXISTS pipeline_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at       TEXT NOT NULL,
            status       TEXT NOT NULL,
            rows_loaded  INTEGER,
            notes        TEXT
        );
    """,
}

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_daily_location ON daily_stats(location);",
    "CREATE INDEX IF NOT EXISTS idx_daily_date     ON daily_stats(date);",
    "CREATE INDEX IF NOT EXISTS idx_daily_continent ON daily_stats(continent);",
]


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't exist."""
    cursor = conn.cursor()
    for table_name, ddl in CREATE_STATEMENTS.items():
        cursor.execute(ddl)
        print(f"[database] Table ready: {table_name}")
    for idx_sql in INDEXES:
        cursor.execute(idx_sql)
    conn.commit()
    print("[database] ✓ Schema created / verified.")


# ------------------------------------------------------------------
# Data loading
# ------------------------------------------------------------------

def load_dataframe(df: pd.DataFrame,
                   table_name: str,
                   conn: sqlite3.Connection,
                   if_exists: str = "replace") -> int:
    """
    Write a DataFrame to a SQLite table.

    Parameters
    ----------
    df         : DataFrame to write
    table_name : Target table name
    conn       : SQLite connection
    if_exists  : 'replace', 'append', or 'fail'

    Returns
    -------
    int
        Number of rows written.
    """
    # Convert date columns to strings for SQLite compatibility
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64"]):
        df[col] = df[col].astype(str)

    # Also handle Period dtype if present
    for col in df.columns:
        if hasattr(df[col], "dt") and hasattr(df[col].dt, "to_timestamp"):
            df[col] = df[col].astype(str)

    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    row_count = len(df)
    print(f"[database]   Loaded {row_count:,} rows → '{table_name}'")
    return row_count


def log_pipeline_run(conn: sqlite3.Connection,
                     status: str,
                     rows_loaded: int,
                     notes: str = "") -> None:
    """Record a pipeline run in the pipeline_log table."""
    conn.execute(
        "INSERT INTO pipeline_log (run_at, status, rows_loaded, notes) VALUES (?, ?, ?, ?)",
        (datetime.now().isoformat(), status, rows_loaded, notes)
    )
    conn.commit()


# ------------------------------------------------------------------
# Query helpers (used by analysis.py and dashboard)
# ------------------------------------------------------------------

def query(sql: str, conn: sqlite3.Connection = None) -> pd.DataFrame:
    """
    Run a SQL query and return the result as a DataFrame.
    If no connection is provided, opens one automatically.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        df = pd.read_sql_query(sql, conn)
        return df
    finally:
        if own_conn:
            conn.close()


def get_table(table_name: str, conn: sqlite3.Connection = None) -> pd.DataFrame:
    """Load a full table into a DataFrame."""
    return query(f"SELECT * FROM {table_name}", conn)


def get_country_list(conn: sqlite3.Connection = None) -> list:
    """Return sorted list of all countries in the database."""
    df = query("SELECT DISTINCT location FROM daily_stats ORDER BY location", conn)
    return df["location"].tolist()


# ------------------------------------------------------------------
# Main load step
# ------------------------------------------------------------------

def load_to_database(processed_data: dict) -> None:
    """
    Load all processed DataFrames into SQLite.
    Called from main.py after processing.

    Parameters
    ----------
    processed_data : dict
        Output from processing.process() — keys map to table names.
    """
    print("\n" + "="*60)
    print("  STEP 3 — DATABASE LOADING")
    print("="*60)

    conn = get_connection()
    total_rows = 0

    try:
        create_schema(conn)
        print()

        table_map = {
            "daily"          : "daily_stats",
            "country_totals" : "country_totals",
            "continent"      : "continent_summary",
            "global_daily"   : "global_daily",
        }

        for data_key, table_name in table_map.items():
            if data_key in processed_data:
                rows = load_dataframe(
                    processed_data[data_key],
                    table_name,
                    conn,
                    if_exists="replace"
                )
                total_rows += rows
            else:
                print(f"[database] Warning: key '{data_key}' not found in processed data.")

        log_pipeline_run(conn, "SUCCESS", total_rows)
        print(f"\n[database] ✓ Pipeline complete. {total_rows:,} total rows loaded.")
        print(f"[database]   Database location: {config.DB_PATH}")

    except Exception as e:
        log_pipeline_run(conn, "FAILED", 0, notes=str(e))
        conn.close()
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    # Quick test: connect and list tables
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    print("Tables in database:", tables)
    conn.close()

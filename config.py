"""
config.py
---------
Central configuration for the COVID-19 data pipeline.
All paths, URLs, and settings are defined here so they
don't get scattered across the project.

Author : Chandra Kanth Darapeneni
Date   : September 2023
"""

import os

# ------------------------------------------------------------------
# Project root (wherever this file lives)
# ------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ------------------------------------------------------------------
# Directory paths
# ------------------------------------------------------------------
DATA_DIR    = os.path.join(BASE_DIR, "data")
OUTPUT_DIR  = os.path.join(BASE_DIR, "outputs")
DB_DIR      = os.path.join(BASE_DIR, "data")

# Make sure dirs exist
os.makedirs(DATA_DIR,   exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------------------------
# Data source
# ------------------------------------------------------------------
# Our World in Data provides a single CSV that is updated daily.
# We download it once and store it locally.
OWID_COVID_URL = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
RAW_CSV_PATH   = os.path.join(DATA_DIR, "owid_covid_raw.csv")

# ------------------------------------------------------------------
# Database
# ------------------------------------------------------------------
DB_PATH = os.path.join(DB_DIR, "covid_warehouse.db")

# Table names inside SQLite
TABLES = {
    "raw"          : "raw_covid",
    "daily"        : "daily_stats",
    "country_totals": "country_totals",
    "continent"    : "continent_summary",
}

# ------------------------------------------------------------------
# Processing settings
# ------------------------------------------------------------------

# OWID adds rows for aggregated groups (continents, income levels, etc.)
# These are NOT real countries and should be excluded from country-level analysis.
NON_COUNTRY_LOCATIONS = [
    "World",
    "Africa",
    "Asia",
    "Europe",
    "European Union",
    "North America",
    "Oceania",
    "South America",
    "High income",
    "Upper middle income",
    "Lower middle income",
    "Low income",
    "International",
]

# Columns we actually care about (drop the rest to save space)
COLUMNS_TO_KEEP = [
    "iso_code",
    "continent",
    "location",
    "date",
    "total_cases",
    "new_cases",
    "new_cases_smoothed",
    "total_deaths",
    "new_deaths",
    "new_deaths_smoothed",
    "total_cases_per_million",
    "new_cases_per_million",
    "total_deaths_per_million",
    "reproduction_rate",
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "total_boosters",
    "people_vaccinated_per_hundred",
    "people_fully_vaccinated_per_hundred",
    "population",
    "hospital_patients",
    "icu_patients",
    "stringency_index",
]

# Date range for the project
# We use data from the start of the pandemic through early 2023
DATE_START = "2020-01-01"
DATE_END   = "2023-10-01"

# ------------------------------------------------------------------
# Dashboard settings
# ------------------------------------------------------------------
DASH_HOST = "127.0.0.1"
DASH_PORT = 8050
DASH_DEBUG = True   # set to False for production

# Top N countries to show in bar charts
TOP_N_COUNTRIES = 20

# Color scheme used throughout the dashboard
COLORS = {
    "background"    : "#0d1117",
    "card_bg"       : "#161b22",
    "accent_blue"   : "#58a6ff",
    "accent_orange" : "#f78166",
    "accent_green"  : "#3fb950",
    "accent_yellow" : "#d29922",
    "text_primary"  : "#e6edf3",
    "text_secondary": "#8b949e",
    "border"        : "#30363d",
}

# Continent color mapping for consistent charts
CONTINENT_COLORS = {
    "Asia"         : "#58a6ff",
    "Europe"       : "#3fb950",
    "North America": "#f78166",
    "South America": "#d29922",
    "Africa"       : "#bc8cff",
    "Oceania"      : "#39d353",
}

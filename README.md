# 🦠 COVID-19 Global Data Engineering & Dashboard
**A personal data engineering portfolio project | 2023**
**Author: Chandra Kanth Darapeneni**

---

## 📌 About This Project

A self-driven data engineering project I built to practice end-to-end pipeline skills — from raw data ingestion to an interactive analytical dashboard. The topic (COVID-19) gave me a real-world dataset with interesting challenges: missing values, inconsistent reporting across countries, and a need to handle both daily and cumulative metrics correctly.

The dataset is sourced from **Our World in Data (OWID)**, which aggregates WHO and national health ministry reports worldwide and is one of the most complete free COVID datasets available.

---

## 🗂️ Project Structure

```
covid_project/
│
├── src/
│   ├── ingestion.py        # Data download & raw CSV handling
│   ├── processing.py       # Cleaning, transforming, feature engineering
│   ├── database.py         # SQLite schema creation & data loading
│   ├── analysis.py         # Aggregations, statistics, KPI calculations
│   └── visualizations.py   # Chart generation with Plotly
│
├── dashboard/
│   └── app.py              # Plotly Dash interactive web dashboard
│
├── data/
│   └── (raw CSVs stored here after ingestion)
│
├── outputs/
│   └── (generated charts saved here)
│
├── config.py               # All project settings & paths
├── main.py                 # Pipeline orchestrator — run this
├── requirements.txt
└── README.md
```

---

## ⚙️ How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the full data pipeline
```bash
python main.py
```
This will:
- Download the OWID COVID dataset
- Clean and transform the data
- Load it into a local SQLite database
- Run analysis and export summary stats

### 3. Launch the interactive dashboard
```bash
python dashboard/app.py
```
Then open your browser at: `http://127.0.0.1:8050`

---

## 🔧 Technologies Used

| Tool | Purpose |
|------|---------|
| Python 3.10 | Core language |
| Pandas | Data manipulation |
| SQLite3 | Local data warehouse |
| Plotly | Chart generation |
| Dash | Interactive web dashboard |
| Requests | HTTP data download |
| NumPy | Numerical computations |

---

## 📊 Dashboard Features

- **Global KPI Cards** — Total cases, deaths, vaccinations, case fatality rate
- **World Map** — Choropleth showing total cases by country
- **Time Series** — Daily new cases / deaths with country filter
- **Top 20 Countries** — Bar chart ranked by total cases
- **Vaccination Progress** — People vaccinated vs population by country
- **Case Fatality Rate** — CFR comparison across regions
- **7-Day Rolling Average** — Smoothed trend lines
- **Continent Breakdown** — Pie chart of cases by continent

---

## 📁 Data Source

- **Our World in Data**: https://ourworldindata.org/covid-cases
- **Raw dataset URL**: https://covid.ourworldindata.org/data/owid-covid-data.csv
- Dataset last fetched: **November 2023**
- Covers: Jan 2020 — Oct 2023

---

## ⚠️ Known Limitations

- Data completeness varies by country (some nations have inconsistent reporting)
- Vaccination data is missing for some low-income countries
- The pipeline uses a local SQLite DB (not production-grade; would use PostgreSQL in a real system)
- Dashboard is not deployed (runs locally only)

---

## 🧠 What I Learned / Challenges

The hardest part honestly was dealing with all the null values in the dataset. Pandas kept throwing warnings when I tried to do rolling averages on columns that were mostly NaN for some countries — had to use `min_periods=1` to get around it. Also took me a while to figure out that OWID includes these aggregate rows like "World", "High income", "European Union" etc. mixed in with actual countries, all using the same column format. Spent probably two hours wondering why my country count was 250+ before I noticed the iso_code for those starts with "OWID_".

The dashboard was the most time-consuming part. I originally tried building it with just Plotly figures exported to static HTML, but then I wanted the country dropdown to be interactive so I had to learn Dash. The callback system was confusing at first — kept getting circular dependency errors until I separated the data loading from the chart update logic.

One thing I would improve if I had more time: right now the whole dataset is re-queried from SQLite every time a dropdown changes. Caching it in memory with `@lru_cache` or Dash's `dcc.Store` would make the dashboard noticeably faster.

---

*Last updated: November 28, 2023*

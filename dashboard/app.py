"""
dashboard/app.py
----------------
Interactive COVID-19 dashboard built with Plotly Dash.

This is the main deliverable of the project — an interactive web app
that lets you explore global COVID-19 data through filters and charts.

Features:
  - Global KPI summary cards
  - World choropleth map (switchable metric)
  - Global time series with date range slider
  - Country-level drilldown
  - Top 20 countries bar chart
  - Continent breakdown donut
  - Vaccination progress
  - Monthly case/death histogram

I originally built the charts as standalone Plotly HTML exports
(see visualizations.py) but wanted real interactivity so moved to Dash.
The callback system was harder to understand than I expected — the
Input/Output decorator is not obvious when you first see it. Also had
to be careful not to reload data from the DB inside every callback or
the dashboard gets very slow. Data is loaded once at startup instead.

Run: python dashboard/app.py
Then open: http://127.0.0.1:8050

Author : Chandra Kanth Darapeneni
Date   : November 2023
"""

import sys
import os

# Make project root importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd

import config
from src.analysis import (
    get_global_kpis,
    get_top_countries_by_cases,
    get_top_countries_by_deaths,
    get_global_time_series,
    get_country_time_series,
    get_continent_breakdown,
    get_vaccination_leaders,
    get_choropleth_data,
    get_monthly_global,
    get_cases_by_continent_over_time,
)
from src.database   import get_country_list
from src.visualizations import (
    plot_global_time_series,
    plot_top_countries,
    plot_choropleth,
    plot_continent_pie,
    plot_vaccination_leaders,
    plot_monthly_cases,
    apply_dark_theme,
    C,
)

# ------------------------------------------------------------------
# App initialisation
# ------------------------------------------------------------------

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.DARKLY,
        "https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=IBM+Plex+Sans:wght@300;400;600&display=swap",
    ],
    title="COVID-19 Global Dashboard | 2023",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    suppress_callback_exceptions=True,
)


# ------------------------------------------------------------------
# Helper: format big numbers nicely
# ------------------------------------------------------------------

def fmt(n, decimals=0):
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "N/A"
    if decimals == 0:
        n = int(n)
        if n >= 1_000_000_000:
            return f"{n/1_000_000_000:.2f}B"
        elif n >= 1_000_000:
            return f"{n/1_000_000:.2f}M"
        elif n >= 1_000:
            return f"{n/1_000:.1f}K"
        return f"{n:,}"
    return f"{n:.{decimals}f}"


# ------------------------------------------------------------------
# Load data at startup (cached — no need to re-query on every callback)
# ------------------------------------------------------------------

try:
    kpis           = get_global_kpis()
    country_list   = get_country_list()
    df_continent   = get_continent_breakdown()
    df_top20       = get_top_countries_by_cases(20)
    df_vax         = get_vaccination_leaders(20)
    df_global_ts   = get_global_time_series()
    df_monthly     = get_monthly_global()
    df_map         = get_choropleth_data("total_cases")
    DATA_LOADED    = True
    DATA_ERROR     = None
except Exception as e:
    DATA_LOADED  = False
    DATA_ERROR   = str(e)
    kpis         = {}
    country_list = []
    print(f"[dashboard] ERROR loading data: {e}")
    print("[dashboard] Make sure you've run  python main.py  first!")


# ------------------------------------------------------------------
# Custom CSS injected inline
# ------------------------------------------------------------------

CUSTOM_CSS = """
body {
    background-color: #0d1117 !important;
    font-family: 'IBM Plex Sans', sans-serif;
}
.kpi-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 20px;
    text-align: center;
    transition: border-color 0.2s;
}
.kpi-card:hover { border-color: #58a6ff; }
.kpi-value {
    font-family: 'Share Tech Mono', monospace;
    font-size: 2rem;
    font-weight: 600;
    color: #58a6ff;
    margin: 4px 0;
}
.kpi-label {
    font-size: 0.75rem;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 1px;
}
.kpi-sub {
    font-size: 0.8rem;
    color: #3fb950;
    margin-top: 4px;
}
.chart-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 20px;
}
.section-title {
    font-size: 0.7rem;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 2px;
    border-bottom: 1px solid #30363d;
    padding-bottom: 8px;
    margin-bottom: 16px;
}
.dash-dropdown .Select-control {
    background-color: #161b22 !important;
    border-color: #30363d !important;
    color: #e6edf3 !important;
}
.Select-menu-outer { background-color: #161b22 !important; }
.Select-option { color: #e6edf3 !important; }
.rc-slider-track { background-color: #58a6ff !important; }
.rc-slider-handle { border-color: #58a6ff !important; }
"""


# ------------------------------------------------------------------
# KPI cards
# ------------------------------------------------------------------

def make_kpi_card(label: str, value: str, sub: str = "", color: str = C["accent_blue"]):
    return html.Div([
        html.Div(label, className="kpi-label"),
        html.Div(value, className="kpi-value", style={"color": color}),
        html.Div(sub,   className="kpi-sub") if sub else None,
    ], className="kpi-card", style={"flex": "1", "minWidth": "160px"})


# ------------------------------------------------------------------
# Layout
# ------------------------------------------------------------------

def build_layout():
    if not DATA_LOADED:
        return html.Div([
            html.H2("⚠️  Data not found", style={"color": C["accent_orange"], "textAlign": "center", "marginTop": "100px"}),
            html.P(f"Error: {DATA_ERROR}", style={"color": C["text_secondary"], "textAlign": "center"}),
            html.P("Please run  python main.py  first to build the database.",
                   style={"color": C["text_secondary"], "textAlign": "center"}),
        ], style={"background": C["background"], "minHeight": "100vh"})

    return html.Div([

        # Inject custom CSS
        html.Style(CUSTOM_CSS),

        # ---- Header ------------------------------------------------
        html.Div([
            html.Div([
                html.Span("🦠", style={"fontSize": "1.8rem", "marginRight": "12px"}),
                html.Span("COVID-19 Global Dashboard", style={
                    "fontFamily": "'Share Tech Mono', monospace",
                    "fontSize": "1.4rem",
                    "color": C["text_primary"],
                    "fontWeight": "600",
                }),
            ], style={"display": "flex", "alignItems": "center"}),
            html.Div([
                html.Span("Data: Our World in Data  |  ", style={"color": C["text_secondary"], "fontSize": "0.8rem"}),
                html.Span("Jan 2020 — Mar 2023", style={
                    "fontFamily": "'Share Tech Mono', monospace",
                    "fontSize": "0.8rem",
                    "color": C["accent_green"],
                }),
            ]),
        ], style={
            "display"        : "flex",
            "justifyContent" : "space-between",
            "alignItems"     : "center",
            "padding"        : "16px 32px",
            "borderBottom"   : f"1px solid {C['border']}",
            "background"     : C["card_bg"],
            "marginBottom"   : "24px",
        }),

        # ---- Main content ------------------------------------------
        html.Div([

            # ---- KPI Row -------------------------------------------
            html.Div("Global Summary", className="section-title"),
            html.Div([
                make_kpi_card("Total Cases",      fmt(kpis.get("total_cases", 0)),      color=C["accent_blue"]),
                make_kpi_card("Total Deaths",     fmt(kpis.get("total_deaths", 0)),     color=C["accent_orange"]),
                make_kpi_card("Vaccinated (1+ dose)", fmt(kpis.get("people_vaccinated", 0)), color=C["accent_green"]),
                make_kpi_card("Fully Vaccinated", fmt(kpis.get("people_fully_vaccinated", 0)), color=C["accent_green"]),
                make_kpi_card("Case Fatality Rate", f"{kpis.get('cfr_global', 0):.2f}%", color=C["accent_yellow"]),
                make_kpi_card("Countries",        str(kpis.get("countries_affected", 0)), color=C["text_primary"]),
            ], style={
                "display"       : "flex",
                "flexWrap"      : "wrap",
                "gap"           : "12px",
                "marginBottom"  : "28px",
            }),

            # ---- World Map ----------------------------------------
            html.Div([
                html.Div([
                    html.Span("World Map", className="section-title", style={"marginBottom": 0}),
                    dcc.Dropdown(
                        id="map-metric-dropdown",
                        options=[
                            {"label": "Total Cases",           "value": "total_cases"},
                            {"label": "Total Deaths",          "value": "total_deaths"},
                            {"label": "Cases per Million",     "value": "total_cases_per_million"},
                            {"label": "Case Fatality Rate",    "value": "cfr"},
                            {"label": "Vaccination Rate (%)",  "value": "vax_rate_pct"},
                        ],
                        value="total_cases",
                        clearable=False,
                        style={
                            "width"          : "220px",
                            "backgroundColor": C["card_bg"],
                            "color"          : C["text_primary"],
                            "borderColor"    : C["border"],
                            "fontSize"       : "0.85rem",
                        },
                    ),
                ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "12px"}),
                dcc.Graph(id="world-map", config={"displayModeBar": False}),
            ], className="chart-card"),

            # ---- Time Series + Continent Row ----------------------
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("Global Time Series", className="section-title", style={"marginBottom": 0}),
                            dcc.Dropdown(
                                id="ts-metric-dropdown",
                                options=[
                                    {"label": "New Cases (7-day avg)",  "value": "new_cases_7day_avg"},
                                    {"label": "New Deaths (7-day avg)", "value": "new_deaths_7day_avg"},
                                    {"label": "Daily New Cases",        "value": "new_cases"},
                                    {"label": "Daily New Deaths",       "value": "new_deaths"},
                                ],
                                value="new_cases_7day_avg",
                                clearable=False,
                                style={
                                    "width"          : "220px",
                                    "backgroundColor": C["card_bg"],
                                    "color"          : C["text_primary"],
                                    "fontSize"       : "0.85rem",
                                },
                            ),
                        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "12px"}),
                        dcc.Graph(id="global-ts", config={"displayModeBar": False}),
                    ], className="chart-card"),
                ], width=8),

                dbc.Col([
                    html.Div([
                        html.Div("Cases by Continent", className="section-title"),
                        dcc.Graph(
                            id="continent-pie",
                            figure=plot_continent_pie(df_continent, "total_cases"),
                            config={"displayModeBar": False},
                        ),
                    ], className="chart-card"),
                ], width=4),
            ], className="mb-3"),

            # ---- Top 20 + Vaccination Row -------------------------
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("Top Countries", className="section-title", style={"marginBottom": 0}),
                            dcc.Dropdown(
                                id="top-metric-dropdown",
                                options=[
                                    {"label": "By Total Cases",  "value": "cases"},
                                    {"label": "By Total Deaths", "value": "deaths"},
                                ],
                                value="cases",
                                clearable=False,
                                style={
                                    "width"          : "180px",
                                    "backgroundColor": C["card_bg"],
                                    "fontSize"       : "0.85rem",
                                },
                            ),
                        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "12px"}),
                        dcc.Graph(id="top-countries-bar", config={"displayModeBar": False}),
                    ], className="chart-card"),
                ], width=6),

                dbc.Col([
                    html.Div([
                        html.Div("Vaccination Progress — Top 20", className="section-title"),
                        dcc.Graph(
                            id="vaccination-chart",
                            figure=plot_vaccination_leaders(df_vax),
                            config={"displayModeBar": False},
                        ),
                    ], className="chart-card"),
                ], width=6),
            ], className="mb-3"),

            # ---- Country Drilldown --------------------------------
            html.Div([
                html.Div([
                    html.Span("Country Drilldown", className="section-title", style={"marginBottom": 0}),
                    dcc.Dropdown(
                        id="country-dropdown",
                        options=[{"label": c, "value": c} for c in country_list],
                        value="United States",
                        clearable=False,
                        style={
                            "width"          : "280px",
                            "backgroundColor": C["card_bg"],
                            "fontSize"       : "0.85rem",
                        },
                    ),
                ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "12px"}),
                dcc.Graph(id="country-ts", config={"displayModeBar": False}),
            ], className="chart-card"),

            # ---- Monthly Overview --------------------------------
            html.Div([
                html.Div("Monthly Global Overview", className="section-title"),
                dcc.Graph(
                    figure=plot_monthly_cases(df_monthly),
                    config={"displayModeBar": False},
                ),
            ], className="chart-card"),

            # ---- Footer ------------------------------------------
            html.Div([
                html.P(
                    "Personal Project | Chandra Kanth Darapeneni | "
                    "Source: Our World in Data (OWID)",
                    style={"color": C["text_secondary"], "fontSize": "0.75rem", "textAlign": "center", "margin": 0},
                ),
            ], style={"padding": "20px", "borderTop": f"1px solid {C['border']}"}),

        ], style={"maxWidth": "1400px", "margin": "0 auto", "padding": "0 24px"}),

    ], style={"background": C["background"], "minHeight": "100vh"})


app.layout = build_layout


# ------------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------------

@app.callback(
    Output("world-map", "figure"),
    Input("map-metric-dropdown", "value"),
)
def update_map(metric):
    label_map = {
        "total_cases"            : "Global COVID-19 Total Cases",
        "total_deaths"           : "Global COVID-19 Total Deaths",
        "total_cases_per_million": "Cases per Million Population",
        "cfr"                    : "Case Fatality Rate (%)",
        "vax_rate_pct"           : "Vaccination Rate (% Population)",
    }
    df = get_choropleth_data(metric)
    return plot_choropleth(df, metric=metric, title=label_map.get(metric, metric))


@app.callback(
    Output("global-ts", "figure"),
    Input("ts-metric-dropdown", "value"),
)
def update_global_ts(metric):
    return plot_global_time_series(df_global_ts, metric=metric)


@app.callback(
    Output("top-countries-bar", "figure"),
    Input("top-metric-dropdown", "value"),
)
def update_top_countries(selection):
    if selection == "cases":
        df  = get_top_countries_by_cases(20)
        return plot_top_countries(df, metric="total_cases", title="Top 20 by Total Cases")
    else:
        df  = get_top_countries_by_deaths(20)
        return plot_top_countries(df, metric="total_deaths", title="Top 20 by Total Deaths")


@app.callback(
    Output("country-ts", "figure"),
    Input("country-dropdown", "value"),
)
def update_country_ts(country):
    if not country:
        return go.Figure()

    df = get_country_time_series(country)
    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            title=dict(text=f"No data found for {country}", font=dict(color=C["text_secondary"])),
            **{k: v for k, v in {
                "paper_bgcolor": C["background"],
                "plot_bgcolor" : C["card_bg"],
            }.items()}
        )
        return fig

    fig = go.Figure()

    # New cases (bar)
    fig.add_trace(go.Bar(
        x    = df["date"],
        y    = df["new_cases"],
        name = "Daily Cases",
        marker_color = C["accent_blue"],
        opacity = 0.4,
        yaxis = "y1",
        hovertemplate = "<b>%{x|%b %d, %Y}</b><br>Cases: %{y:,.0f}<extra></extra>",
    ))

    # 7-day average
    fig.add_trace(go.Scatter(
        x    = df["date"],
        y    = df["new_cases_7day_avg"],
        name = "7-day Avg",
        mode = "lines",
        line = dict(color=C["accent_blue"], width=2.5),
        yaxis = "y1",
        hovertemplate = "<b>%{x|%b %d, %Y}</b><br>7-day avg: %{y:,.0f}<extra></extra>",
    ))

    # Deaths on secondary axis
    fig.add_trace(go.Scatter(
        x    = df["date"],
        y    = df["new_deaths_7day_avg"],
        name = "Deaths (7-day avg)",
        mode = "lines",
        line = dict(color=C["accent_orange"], width=2, dash="dot"),
        yaxis = "y2",
        hovertemplate = "<b>%{x|%b %d, %Y}</b><br>Deaths 7-day avg: %{y:,.0f}<extra></extra>",
    ))

    # Vaccination % on tertiary axis (if available)
    if "people_vaccinated_per_hundred" in df.columns:
        df_vax_country = df[df["people_vaccinated_per_hundred"] > 0]
        if not df_vax_country.empty:
            fig.add_trace(go.Scatter(
                x    = df_vax_country["date"],
                y    = df_vax_country["people_vaccinated_per_hundred"],
                name = "Vaccinated (%)",
                mode = "lines",
                line = dict(color=C["accent_green"], width=1.5, dash="dash"),
                yaxis = "y3",
                hovertemplate = "<b>%{x|%b %d, %Y}</b><br>Vaccinated: %{y:.1f}%<extra></extra>",
            ))

    fig.update_layout(
        title = dict(text=f"{country} — COVID-19 Timeline", font=dict(size=14, color=C["text_primary"])),
        height = 420,
        hovermode = "x unified",
        yaxis  = dict(title="Cases", side="left"),
        yaxis2 = dict(title="Deaths", overlaying="y", side="right", showgrid=False),
        yaxis3 = dict(title="Vax %",  overlaying="y", side="right",
                      position=0.97, showgrid=False, range=[0, 105]),
        legend = dict(orientation="h", y=-0.15),
    )
    return apply_dark_theme(fig)


# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------

if __name__ == "__main__":
    print(f"\n[dashboard] Starting server at http://{config.DASH_HOST}:{config.DASH_PORT}")
    print("[dashboard] Press Ctrl+C to stop.\n")
    app.run_server(
        host  = config.DASH_HOST,
        port  = config.DASH_PORT,
        debug = config.DASH_DEBUG,
    )

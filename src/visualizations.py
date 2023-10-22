"""
src/visualizations.py
---------------------
Standalone chart generation using Plotly.
These functions return Plotly Figure objects that can be:
  - Displayed in Jupyter notebooks (.show())
  - Used directly in the Dash dashboard
  - Exported to PNG/HTML with fig.write_image() or fig.write_html()

I separated this from the dashboard (app.py) so charts can be
re-used or exported without running the full Dash server.

Author : Chandra Kanth Darapeneni
Date   : November 2023
"""

import sys
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# Pull colors from config
C = config.COLORS


# ------------------------------------------------------------------
# Shared layout / theme
# ------------------------------------------------------------------

DARK_LAYOUT = dict(
    paper_bgcolor = C["background"],
    plot_bgcolor  = C["card_bg"],
    font          = dict(family="'Courier New', monospace", color=C["text_primary"], size=12),
    margin        = dict(l=40, r=20, t=50, b=40),
    legend        = dict(
        bgcolor     = C["card_bg"],
        bordercolor = C["border"],
        borderwidth = 1,
        font        = dict(color=C["text_secondary"]),
    ),
    xaxis = dict(
        gridcolor   = C["border"],
        linecolor   = C["border"],
        tickcolor   = C["text_secondary"],
        tickfont    = dict(color=C["text_secondary"]),
    ),
    yaxis = dict(
        gridcolor   = C["border"],
        linecolor   = C["border"],
        tickcolor   = C["text_secondary"],
        tickfont    = dict(color=C["text_secondary"]),
    ),
)


def apply_dark_theme(fig: go.Figure) -> go.Figure:
    """Apply the dark theme to any figure."""
    fig.update_layout(**DARK_LAYOUT)
    return fig


def fmt_number(n: float) -> str:
    """Format large numbers for axis labels and tooltips."""
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B"
    elif n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


# ------------------------------------------------------------------
# 1. Global time series
# ------------------------------------------------------------------

def plot_global_time_series(df: pd.DataFrame,
                            metric: str = "new_cases_7day_avg") -> go.Figure:
    """
    Line chart of global daily new cases or deaths over time.

    Parameters
    ----------
    df     : output from analysis.get_global_time_series()
    metric : column to plot
    """
    color_map = {
        "new_cases_7day_avg"  : C["accent_blue"],
        "new_deaths_7day_avg" : C["accent_orange"],
        "new_cases"           : C["accent_blue"],
        "new_deaths"          : C["accent_orange"],
    }
    label_map = {
        "new_cases_7day_avg"  : "New Cases (7-day avg)",
        "new_deaths_7day_avg" : "New Deaths (7-day avg)",
        "new_cases"           : "Daily New Cases",
        "new_deaths"          : "Daily New Deaths",
    }

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x    = df["date"],
        y    = df[metric],
        name = label_map.get(metric, metric),
        mode = "lines",
        line = dict(color=color_map.get(metric, C["accent_blue"]), width=2),
        fill = "tozeroy",
        fillcolor = color_map.get(metric, C["accent_blue"]).replace(")", ", 0.15)").replace("rgb", "rgba") if "rgb" in color_map.get(metric, "") else None,
        hovertemplate = "<b>%{x|%b %d, %Y}</b><br>%{y:,.0f}<extra></extra>",
    ))

    fig.update_layout(
        title = dict(text=label_map.get(metric, metric), font=dict(size=14, color=C["text_primary"])),
        xaxis_title = None,
        yaxis_title = None,
        hovermode   = "x unified",
    )
    return apply_dark_theme(fig)


# ------------------------------------------------------------------
# 2. Top countries bar chart
# ------------------------------------------------------------------

def plot_top_countries(df: pd.DataFrame,
                       metric: str = "total_cases",
                       title : str = "Top 20 Countries by Total Cases") -> go.Figure:
    """
    Horizontal bar chart of top countries.
    """
    continent_colors = {k: v for k, v in config.CONTINENT_COLORS.items()}

    df = df.sort_values(metric, ascending=True).copy()

    colors = [continent_colors.get(c, C["accent_blue"]) for c in df["continent"]]

    fig = go.Figure(go.Bar(
        x           = df[metric],
        y           = df["location"],
        orientation = "h",
        marker_color= colors,
        text        = df[metric].apply(fmt_number),
        textposition= "outside",
        textfont    = dict(color=C["text_secondary"], size=10),
        hovertemplate = "<b>%{y}</b><br>%{x:,.0f}<extra></extra>",
    ))

    fig.update_layout(
        title  = dict(text=title, font=dict(size=14, color=C["text_primary"])),
        height = 600,
        xaxis  = dict(tickformat=","),
        yaxis  = dict(tickfont=dict(size=10)),
    )
    return apply_dark_theme(fig)


# ------------------------------------------------------------------
# 3. Choropleth world map
# ------------------------------------------------------------------

def plot_choropleth(df: pd.DataFrame,
                   metric: str = "total_cases",
                   title : str = "Global COVID-19 Cases") -> go.Figure:
    """
    World choropleth map coloured by the chosen metric.
    """
    label_map = {
        "total_cases"            : "Total Cases",
        "total_deaths"           : "Total Deaths",
        "cfr"                    : "Case Fatality Rate (%)",
        "vax_rate_pct"           : "Vaccination Rate (%)",
        "total_cases_per_million": "Cases per Million",
    }

    fig = px.choropleth(
        df,
        locations      = "iso_code",
        color          = "value",
        hover_name     = "location",
        color_continuous_scale = "Reds" if "cases" in metric or "deaths" in metric else "Blues",
        range_color    = [0, df["value"].quantile(0.95)],
        labels         = {"value": label_map.get(metric, metric)},
    )

    fig.update_traces(
        hovertemplate = (
            "<b>%{hovertext}</b><br>"
            f"{label_map.get(metric, metric)}: %{{z:,.0f}}<extra></extra>"
        )
    )

    fig.update_layout(
        title      = dict(text=title, font=dict(size=14, color=C["text_primary"])),
        geo        = dict(
            bgcolor       = C["background"],
            showframe     = False,
            showcoastlines= True,
            coastlinecolor= C["border"],
            showland      = True,
            landcolor     = C["card_bg"],
            showocean     = True,
            oceancolor    = C["background"],
            projection_type = "natural earth",
        ),
        coloraxis_colorbar = dict(
            tickfont = dict(color=C["text_secondary"]),
            title    = dict(font=dict(color=C["text_secondary"])),
            bgcolor  = C["card_bg"],
        ),
        height = 450,
    )
    return apply_dark_theme(fig)


# ------------------------------------------------------------------
# 4. Continent pie / donut
# ------------------------------------------------------------------

def plot_continent_pie(df: pd.DataFrame, metric: str = "total_cases") -> go.Figure:
    """Donut chart of cases/deaths split by continent."""
    colors = [config.CONTINENT_COLORS.get(c, C["accent_blue"]) for c in df["continent"]]

    fig = go.Figure(go.Pie(
        labels      = df["continent"],
        values      = df[metric],
        hole        = 0.5,
        marker_colors = colors,
        textfont    = dict(color=C["text_primary"]),
        hovertemplate = "<b>%{label}</b><br>%{value:,.0f} (%{percent})<extra></extra>",
    ))

    fig.update_layout(
        title  = dict(text="Cases by Continent", font=dict(size=14, color=C["text_primary"])),
        height = 380,
        showlegend = True,
    )
    return apply_dark_theme(fig)


# ------------------------------------------------------------------
# 5. Vaccination progress
# ------------------------------------------------------------------

def plot_vaccination_leaders(df: pd.DataFrame) -> go.Figure:
    """
    Grouped bar chart: partial vs full vaccination per country.
    """
    df = df.sort_values("people_vaccinated_per_hundred", ascending=True)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name  = "At least 1 dose",
        x     = df["people_vaccinated_per_hundred"],
        y     = df["location"],
        orientation = "h",
        marker_color = C["accent_blue"],
        hovertemplate = "<b>%{y}</b><br>1+ dose: %{x:.1f}%<extra></extra>",
    ))

    if "people_fully_vaccinated_per_hundred" in df.columns:
        fig.add_trace(go.Bar(
            name  = "Fully vaccinated",
            x     = df["people_fully_vaccinated_per_hundred"],
            y     = df["location"],
            orientation = "h",
            marker_color = C["accent_green"],
            hovertemplate = "<b>%{y}</b><br>Fully vaccinated: %{x:.1f}%<extra></extra>",
        ))

    fig.update_layout(
        title    = dict(text="Vaccination Progress — Top 20 Countries", font=dict(size=14, color=C["text_primary"])),
        barmode  = "overlay",
        height   = 550,
        xaxis    = dict(title="% of Population", range=[0, 105]),
        bargap   = 0.15,
    )
    return apply_dark_theme(fig)


# ------------------------------------------------------------------
# 6. Monthly bar chart
# ------------------------------------------------------------------

def plot_monthly_cases(df: pd.DataFrame) -> go.Figure:
    """Monthly global case counts as a bar chart."""
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x           = df["year_month"],
        y           = df["monthly_cases"],
        name        = "Monthly Cases",
        marker_color = C["accent_blue"],
        hovertemplate = "<b>%{x}</b><br>Cases: %{y:,.0f}<extra></extra>",
    ))

    fig.add_trace(go.Bar(
        x           = df["year_month"],
        y           = df["monthly_deaths"],
        name        = "Monthly Deaths",
        marker_color = C["accent_orange"],
        yaxis       = "y2",
        hovertemplate = "<b>%{x}</b><br>Deaths: %{y:,.0f}<extra></extra>",
    ))

    fig.update_layout(
        title   = dict(text="Monthly Global Cases & Deaths", font=dict(size=14, color=C["text_primary"])),
        barmode = "group",
        height  = 380,
        yaxis   = dict(title="Cases"),
        yaxis2  = dict(title="Deaths", overlaying="y", side="right"),
        xaxis   = dict(tickangle=-45, tickfont=dict(size=9)),
    )
    return apply_dark_theme(fig)


# ------------------------------------------------------------------
# Export all static charts
# ------------------------------------------------------------------

def export_all_charts(output_dir: str = config.OUTPUT_DIR) -> None:
    """
    Generate and save all charts as HTML files.
    PNG export requires kaleido to be installed; HTML always works.
    """
    from src.analysis import (
        get_global_time_series, get_top_countries_by_cases,
        get_continent_breakdown, get_vaccination_leaders,
        get_choropleth_data, get_monthly_global,
    )

    os.makedirs(output_dir, exist_ok=True)
    charts = {
        "global_time_series.html"  : plot_global_time_series(get_global_time_series()),
        "top20_countries.html"     : plot_top_countries(get_top_countries_by_cases()),
        "world_map.html"           : plot_choropleth(get_choropleth_data()),
        "continent_pie.html"       : plot_continent_pie(get_continent_breakdown()),
        "vaccination.html"         : plot_vaccination_leaders(get_vaccination_leaders()),
        "monthly_cases.html"       : plot_monthly_cases(get_monthly_global()),
    }

    for filename, fig in charts.items():
        path = os.path.join(output_dir, filename)
        fig.write_html(path, include_plotlyjs="cdn")
        print(f"[viz] Saved: {path}")

    print(f"\n[viz] ✓ All charts exported to {output_dir}")

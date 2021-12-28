#!/usr/bin/env python3
# coding: utf-8
"""
Aggregates key data from Zoe and government to provide dashboards for
datasets on a single chart, per region.

Interactive: click on dataset in bottom (bar) chart to include this dataset,
brush date interval in top (line) chart to select data from those dates.
"""
import os
from pathlib import Path
import sys

import altair as alt
from altair_saver import save
from bs4 import BeautifulSoup, Doctype

from modules.dataframe_builder import make_default_dataframes

sys.path.append("../src")

RUNPATH = Path(__file__)
SRC_DIRECTORY = RUNPATH.parent
HEADPATH = SRC_DIRECTORY.parent
CHARTS_DIRECTORY = HEADPATH / "html_charts"

# slice to 1st wave, say 1st March
START = "2020-03-01"

to_plot, regions_to_use = make_default_dataframes()

for region in regions_to_use:
    source = (
        to_plot[~(to_plot.index < START)]
        .swaplevel(axis=1)[region]
        .reset_index()
        .melt(id_vars=["date"])
    )
    source_average = (
        to_plot.rolling(window=7, center=True)
        .mean()[~(to_plot.index < START)]
        .swaplevel(axis=1)[region]
        .reset_index()
        .melt(id_vars=["date"])
    )

    # excluding Inpatients as its not a cumulative figure
    source = source[source.variable != "Inpatients"]

    y_ticks_logarithmic = [1, 10, 100, 1000, 10000]

    variable_list = list(source.variable.unique())
    color_list = [
        "#3b84c1",
        "green",
        "red",
        "#9d3abd",
        "brown",
    ]
    assert len(variable_list) == len(color_list)

    scale = alt.Scale(domain=variable_list, range=color_list)
    color = alt.Color(
        "variable:N",
        scale=scale,
    )

    brush = alt.selection_interval(encodings=["x"], name="sel")
    click = alt.selection_multi(encodings=["color"])

    line = (
        alt.Chart(source, title=f"{region}: Covid-19 key data, log scale")
        .mark_line(interpolate="basis")
        .encode(
            x=alt.X(
                "date:T",
                axis=alt.Axis(title=""),
            ),
            y=alt.Y(
                "value:Q",
                scale=alt.Scale(
                    type="symlog",
                ),
                axis=alt.Axis(
                    tickCount=10,
                    orient="right",
                    title="",
                    values=y_ticks_logarithmic,
                ),
            ),
            color="variable:N",
            strokeWidth=alt.value(0.25),
        )
    )

    line_average = (
        alt.Chart(source_average)
        .mark_line(interpolate="basis")
        .encode(
            x=alt.X(
                "date:T",
                axis=alt.Axis(title=""),
            ),
            y=alt.Y(
                "value:Q",
                scale=alt.Scale(
                    type="symlog",
                ),
                axis=alt.Axis(
                    tickCount=10,
                    orient="right",
                    title="",
                    values=y_ticks_logarithmic,
                ),
            ),
            strokeWidth=alt.value(3),
            color="variable:N",
            # can't get this working
            # color=alt.condition(brush, color, alt.value('lightgray')),
        )
        .add_selection(brush)
        .transform_filter(click)
    )

    nearest = alt.selection(
        type="single",
        nearest=True,
        on="mouseover",
        fields=["date"],
        empty="none",
    )

    # Transparent selectors across the chart. This is what tells us
    # the x-value of the cursor
    selectors = (
        line.mark_point()  # type: ignore
        .encode(
            x="date:T",
            opacity=alt.value(0),
        )
        .add_selection(nearest)
    )

    # Draw points on the line, and highlight based on selection
    points = line.mark_point().encode(  # type: ignore
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )

    # Draw text labels near the points, and highlight based on selection
    text = line.mark_text(align="left", dx=5, dy=-15).encode(  # type: ignore
        text=alt.condition(nearest, "value:Q", alt.value(" ")),
    )

    # Draw a rule at the location of the selection
    rules = (
        alt.Chart(source)
        .mark_rule(color="gray")
        .encode(
            x="date:T",
        )
        .transform_filter(nearest)
    )

    # Bottom panel is a bar chart
    bars = (
        alt.Chart(source)
        .mark_bar()
        .encode(
            x=alt.X(
                "sum(value):Q",
                axis=alt.Axis(
                    format=",.0f", title="total over selected period"
                ),
            ),
            y=alt.Y("variable:N", axis=alt.Axis(title="")),
            color=alt.condition(click, color, alt.value("lightgray")),
        )
        .transform_filter(brush)
        .add_selection(click)
        .properties(
            width=800,
        )
    )

    # ~~~~~~~~~~~~~~~~~
    # https://stackoverflow.com/questions/60893866/get-sum-over-selection-in-altair
    brush_text = (
        alt.Chart(source)
        .transform_filter(brush)
        .transform_calculate(
            date_range=(
                "sel.date ? "
                "utcdate(sel.date[0]) + ' '"
                " + monthAbbrevFormat(month(sel.date[0])) + ' '"
                " + utcyear(sel.date[0]) + ' to ' + "
                "utcdate(sel.date[1]) + ' '"
                " + monthAbbrevFormat(month(sel.date[1])) + ' '"
                " + utcyear(sel.date[1])"
                " : ''"
            ),
            text="datum.date_range",
        )
        .mark_text(
            dy=-200,
            size=12,
            baseline="middle",
        )
        .encode(
            text=alt.Text("text:N"),
        )
    )
    # ~~~~~~~~~~~~~~~~~

    bars_text = (
        alt.Chart(source)
        .mark_text(
            align="left",
            baseline="middle",
            dx=3,  # Nudges text to right so it doesn't appear on top of the bar
        )
        .encode(
            x=alt.X("sum(value):Q"),
            y=alt.Y("variable:N"),
            detail="variable:N",
            text=alt.Text("sum(value):Q", format=",.0f"),
        )
        .transform_filter(brush)
    )

    layer = alt.layer(
        line_average,
        line,
        selectors,
        points,
        rules,
        text,
        brush_text,
    ).properties(width=800, height=350)

    layer2 = alt.layer(bars + bars_text,).properties(
        width=800,
    )

    chart = alt.vconcat(
        layer,
        layer2,
    )

    filename = os.path.join(
        CHARTS_DIRECTORY, region.replace(" ", "") + ".html"
    )
    print(f"saving as {filename}")
    save(chart, filename)

    # clean up to remove Doctype line at start so that jekyll can {% include %}
    with open(filename, encoding="utf-8") as fp:
        soup = BeautifulSoup(fp, "html.parser")

    for item in soup.contents:
        if isinstance(item, Doctype):
            item.extract()

    with open(filename, "w", encoding="utf-8") as file:
        file.write(str(soup))

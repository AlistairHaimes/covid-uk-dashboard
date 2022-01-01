#!/usr/bin/env python3
# coding: utf-8
"""
Utils to aggregate dataframes into a single dataframe and chart-related
"""
import pandas as pd
import matplotlib as mpl  # for formatting log y axis
from matplotlib.dates import YearLocator, MonthLocator  # for formatting x axis
import matplotlib.dates as mdates  # for formatting x axis
import matplotlib.pyplot as plt
import seaborn as sns


def aggregate_dataframes(datasets: dict) -> pd.DataFrame:
    """Build a single dataframe from dict of passed dataframes"""
    aggregated_df = pd.concat(
        datasets.values(), keys=datasets.keys(), join="outer", axis=1
    )
    return aggregated_df


def format_ax(ax: plt.subplot, region: str, lgnd_labels: list) -> plt.subplot:
    """Common formatting for all charts"""
    y_ticks_logarithmic = [1, 10, 100, 1000, 10000]
    handles, _ = ax.get_legend_handles_labels()
    ax.legend(
        handles=handles,
        labels=lgnd_labels,
        title="",
        loc="upper left",
        facecolor="white",
    )
    ax.margins(x=0.01)  # 0.05 is default if not specified
    ax.set_yscale("log")
    ax.xaxis.set_label_text("")  # obviously date
    ax.set_yticks(y_ticks_logarithmic)
    if region == "England":
        ax.set_ylim(bottom=1)
    else:
        ax.set_ylim(bottom=1, top=60000)
    
    ax.yaxis.tick_right()
    ax.yaxis.set_label_text(label="")
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
    
    ax.xaxis.set_major_locator(YearLocator(1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    ax.xaxis.set_minor_locator(MonthLocator())
    ax.get_xaxis().set_minor_formatter(mdates.DateFormatter("%b"))
    
    return ax


def line(data, ax, linewidth):
    seaborn_line = sns.lineplot(
        data=data,
        x="date",
        y="value",
        ax=ax,
        linewidth=linewidth,
        hue="variable",
    )
    return seaborn_line

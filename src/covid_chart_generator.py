#!/usr/bin/env python3
# coding: utf-8
"""
Aggregates key data from Zoe and government to provide dashboards for
datasets on a single chart, both nationally and per region.
"""
from pathlib import Path, PurePath

import matplotlib as mpl  # for formatting log y axis
import matplotlib.pyplot as plt
import seaborn as sns

from modules.dataframe_builder import make_default_dataframes
from modules.utils import format_ax, line


RUNPATH = Path(__file__)
SRC_DIRECTORY = RUNPATH.parent
HEADPATH = SRC_DIRECTORY.parent
CHARTS_DIRECTORY = HEADPATH / "charts"


# slice to 1st wave, say 1st March
START = "2020-03-01"

# chart preferences
sns.set_style("ticks")
sns.set_color_codes("dark")
mpl.use("Agg")  # so can save without displaying


def individual_charts(to_plot, regions):
    for region in regions:
        print(f"making chart for {region}")
        fig, ax = plt.subplots(figsize=(14, 6), dpi=300)
        line(
            data=to_plot[~(to_plot.index < START)]
            .swaplevel(axis=1)[region]
            .reset_index()
            .melt(id_vars=["date"]),
            ax=ax,
            linewidth=0.5,
        )
        line(
            data=to_plot.rolling(window=7, center=True)
            .mean()[~(to_plot.index < START)]
            .swaplevel(axis=1)[region]
            .reset_index()
            .melt(id_vars=["date"]),
            ax=ax,
            linewidth=2.0,
        )
        mpl.rcParams["legend.fontsize"] = "medium"
        ax = format_ax(ax, region, list(to_plot.columns.droplevel(1).unique()))
        ax.tick_params(labelsize="large")
        fig.suptitle(
            f"{region}: Covid-19 key data, log scale", y=0.93
        )  # fontweight='bold'
        fig.savefig(
            PurePath(CHARTS_DIRECTORY, region.replace(" ", "") + "KeyData.png")
        )
        plt.close()


def dashboard(to_plot, regions):
    print("making dashboard chart")
    f = plt.figure(constrained_layout=True, figsize=(12, 10), dpi=300)
    gs = f.add_gridspec(4, 2)
    mpl.rcParams["legend.fontsize"] = "xx-small"
    for counter, region in enumerate(regions):
        # create ax in the right place
        ax = f.add_subplot(gs[(counter % 4, counter // 4)])
        line(
            data=to_plot[~(to_plot.index < START)]
            .swaplevel(axis=1)[region]
            .reset_index()
            .melt(id_vars=["date"]),
            ax=ax,
            linewidth=0.15,
        )
        line(
            data=to_plot.rolling(window=7, center=True)
            .mean()[~(to_plot.index < START)]
            .swaplevel(axis=1)[region]
            .reset_index()
            .melt(id_vars=["date"]),
            ax=ax,
            linewidth=1.0,
        )
        ax.tick_params(which="major", labelsize="x-small")
        ax.tick_params(which="minor", labelsize="xx-small")
        ax = format_ax(ax, region, list(to_plot.columns.droplevel(1).unique()))
        ax.set_title(label=region, fontsize="small", fontweight="semibold")

    f.suptitle(
        "England: Covid-19 key data by region, log scale.", fontweight="bold"
    )
    f.savefig(PurePath(CHARTS_DIRECTORY, "KeyRegionalData.png"))
    plt.close()


if __name__ == "__main__":

    aggreg_df, regions_to_use = make_default_dataframes()

    individual_charts(aggreg_df, regions_to_use)
    dashboard(aggreg_df, regions_to_use)

#!/usr/bin/env python3
# coding: utf-8
"""
Aggregates key data from Zoe and government to provide dashboards for
datasets on a single chart, both nationally and per region.
"""

from datetime import datetime, date, timedelta

import pandas as pd
import matplotlib as mpl  # for formatting log y axis
from matplotlib.dates import MonthLocator  # for formatting x axis
import matplotlib.dates as mdates  # for formatting x axis
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from uk_covid19 import Cov19API

# slice to 1st wave, say 1st March
START = "2020-03-01"

# chart preferences
sns.set_style("ticks")
sns.set_color_codes("dark")
mpl.use("Agg")  # so can save without displaying


# Zoe data
def fetch_zoe_data() -> pd.DataFrame:
    """# Zoe's csv is normally dated about 5 days in the past, loop back from
    today's date to get the latest file.
    """
    today = date.today()
    zoe_path = "gcs://covid-public-data/csv/incidence_"
    for i in range(10):
        try:
            try_date = today - timedelta(days=i)
            try_file = zoe_path + (try_date.strftime("%Y%m%d")) + ".csv"
            zoe_dataframe = pd.read_csv(try_file)
            # uncomment to save current file
            # zoe_dataframe.to_csv('zoe.csv')
            print(f"downloaded Zoe data for {try_date.strftime('%d-%m-%Y')}")
            return zoe_dataframe
        except ImportError as missing_gcsfs:
            # probably means gcsfs not installed
            raise ImportError(
                "gcsfs package not installed, required"
            ) from missing_gcsfs
        except FileNotFoundError:
            print(f"no Zoe data for {try_date.strftime('%d-%m-%Y')}")
            continue
    return None


def process_zoe(raw_zoe: pd.DataFrame) -> pd.DataFrame:
    if not raw_zoe.empty:
        raw_zoe.drop(
            raw_zoe.columns.difference(["date", "region", "covid_in_pop"]),
            1,
            inplace=True,
        )
        zoe_dataframe = raw_zoe.pivot_table(
            index="date", columns="region"
        ).apply(np.int64)
        zoe_dataframe.columns = zoe_dataframe.columns.droplevel(0)
        to_drop = ["Northern Ireland", "Scotland", "UK", "Wales"]
        zoe_dataframe = zoe_dataframe.drop(to_drop, axis=1)
        zoe_dataframe.index = pd.to_datetime(
            zoe_dataframe.index, dayfirst=True
        )
        zoe_dataframe["Midlands"] = (
            zoe_dataframe["West Midlands"] + zoe_dataframe["East Midlands"]
        )
        zoe_dataframe["North East and Yorkshire"] = (
            zoe_dataframe["North East"]
            + zoe_dataframe["Yorkshire and The Humber"]
        )
        return zoe_dataframe
    raise FileNotFoundError("Can't get Zoe data")


def call_gov_api(
    callname: str, filters: list, structure: dict
) -> pd.DataFrame:
    """helper function for api calls to UK govt covid19 api"""
    api = Cov19API(
        filters=filters,
        structure=structure,
    )
    dataframe_from_api = api.get_dataframe()
    timestamp = api.last_update
    parsed_timestamp = datetime.fromisoformat(timestamp.strip("Z"))
    print(f"got {callname} data to {parsed_timestamp.strftime('%d-%m-%Y')}")
    # uncomment below if you want a csv
    # dataframe_from_api.to_csv(f'{areaName}Healthcare.csv')
    return dataframe_from_api


# deaths
def fetch_deaths_data():
    deaths_query = {
        "date": "date",
        "region": "areaName",
        "deaths": "newDeaths28DaysByDeathDate",
    }
    deaths_df = call_gov_api(
        callname="deaths", filters=["areaType=region"], structure=deaths_query
    )
    deaths_df.date = pd.to_datetime(deaths_df.date)
    return deaths_df


def process_deaths_data(deaths_df: pd.DataFrame) -> pd.DataFrame:
    ney = (
        deaths_df[
            (deaths_df.region == "North East")
            | (deaths_df.region == "Yorkshire and The Humber")
        ]
        .groupby("date")
        .sum()
    )
    midlands = (
        deaths_df[
            (deaths_df.region == "East Midlands")
            | (deaths_df.region == "West Midlands")
        ]
        .groupby("date")
        .sum()
    )
    ney["region"], midlands["region"] = "North East and Yorkshire", "Midlands"
    deaths_df = deaths_df.set_index("date").append([ney, midlands])
    england = deaths_df.groupby("date").sum().reset_index()
    england["region"] = "England"
    deaths_df = pd.concat([deaths_df.reset_index(), england])
    deaths_df = deaths_df.pivot_table(index="date", columns="region")
    deaths_df.columns = deaths_df.columns.droplevel(0)
    # exclude final 3 days as figures will be updated
    deaths_df = deaths_df.iloc[:-3]
    return deaths_df


# Hospital data
def fetch_healthcare_data(area_name: str) -> pd.DataFrame:
    areas = {
        "england": ["areaType=nation", "areaName=England"],
        "regions": ["areaType=nhsRegion"],
    }
    healthcare_query = {
        "date": "date",
        "region": "areaName",
        "newAdmissions": "newAdmissions",
        "hospitalCases": "hospitalCases",  # inpatients
        #'covidOccupiedMVBeds': 'covidOccupiedMVBeds', # kinda ICU
    }
    return call_gov_api(
        callname=f"healthcare ({area_name})",
        filters=areas[area_name],
        structure=healthcare_query,
    )


def make_healthcare_data():
    healthcare_dfs = pd.concat(
        [fetch_healthcare_data("regions"), fetch_healthcare_data("england")],
        join="inner",
        ignore_index=True,
    )
    healthcare_dfs.date = pd.to_datetime(healthcare_dfs.date)
    admissions_df = healthcare_dfs.pivot_table(
        index="date", columns="region", values="newAdmissions"
    )
    inpatients_df = healthcare_dfs.pivot_table(
        index="date", columns="region", values="hospitalCases"
    )
    return admissions_df, inpatients_df


# Cases
def fetch_cases_data():
    cases_query = {
        "metric": "newCasesBySpecimenDateAgeDemographics",
        "region": "areaName",
        "date": "date",
    }
    cases_dataframe = call_gov_api(
        callname="cases", filters=["areaType=region"], structure=cases_query
    )
    return cases_dataframe.explode("metric")


def process_cases_data(cases_dataframe: pd.DataFrame) -> pd.DataFrame:
    cases_dataframe["age"] = cases_dataframe["metric"].apply(
        lambda x: x["age"]
    )
    cases_dataframe["cases"] = cases_dataframe["metric"].apply(
        lambda x: x["cases"]
    )
    cases_dataframe.date = pd.to_datetime(cases_dataframe.date)
    cases_dataframe = cases_dataframe.set_index("date")
    england = cases_dataframe.groupby(["date", "age"]).sum()
    england["region"] = "England"
    england = england.reset_index().set_index("date")
    cases_dataframe = pd.concat([cases_dataframe, england])
    midlands = pd.concat(
        [
            cases_dataframe[cases_dataframe.region == "East Midlands"],
            cases_dataframe[cases_dataframe.region == "West Midlands"],
        ]
    )
    midlands = (
        midlands.reset_index()
        .groupby(["date", "age"])
        .sum()
        .reset_index()
        .set_index("date")
    )
    midlands["region"] = "Midlands"
    ney = pd.concat(
        [
            cases_dataframe[cases_dataframe.region == "North East"],
            cases_dataframe[
                cases_dataframe.region == "Yorkshire and The Humber"
            ],
        ]
    )
    ney = (
        ney.reset_index()
        .groupby(["date", "age"])
        .sum()
        .reset_index()
        .set_index("date")
    )
    ney["region"] = "North East and Yorkshire"
    cases_dataframe = pd.concat([cases_dataframe, ney, midlands])
    u60_df = (
        cases_dataframe[cases_dataframe.age == "00_59"]
        .pivot_table(index="date", columns="region")
        .cases
    )
    o60_df = (
        cases_dataframe[cases_dataframe.age == "60+"]
        .pivot_table(index="date", columns="region")
        .cases
    )
    cases_df = u60_df + o60_df
    return u60_df, o60_df, cases_df


def aggreg(datasets: dict) -> pd.DataFrame:
    """Build a single dataframe from dict of passed dataframes"""
    aggregated_df = pd.concat(
        datasets.values(), keys=datasets.keys(), join="outer", axis=1
    )
    return aggregated_df


def format_ax(ax: plt.subplot, region: str, lgnd_labels: list) -> plt.subplot:
    """common formatting for all charts"""
    y_ticks_logarithmic = [1, 10, 100, 1000, 10000]  # for log y-scale
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
        ax.set_ylim(bottom=1, top=20000)
    ax.yaxis.tick_right()
    ax.yaxis.set_label_text(label="")
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("{x:,.0f}"))
    ax.xaxis.set_major_locator(MonthLocator())
    ax.get_xaxis().set_major_formatter(mdates.DateFormatter("%b"))
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


def individual_charts(to_plot, regions):
    for region in regions:
        fig, ax = plt.subplots(figsize=(12, 7))
        line(
            data=to_plot[~(to_plot.index < START)]
            .swaplevel(axis=1)[region]
            .reset_index()
            .melt(id_vars=["date"]),
            ax=ax,
            linewidth=0.5,
        )
        line(
            data=to_plot.rolling(window=7)
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
        fig.savefig(region.replace(" ", "") + "KeyData.png", dpi=200)
        plt.close()


def dashboard(to_plot, regions):
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
            data=to_plot.rolling(window=7)
            .mean()[~(to_plot.index < START)]
            .swaplevel(axis=1)[region]
            .reset_index()
            .melt(id_vars=["date"]),
            ax=ax,
            linewidth=1.0,
        )
        ax = format_ax(ax, region, list(to_plot.columns.droplevel(1).unique()))
        ax.set_title(label=region, fontsize="small", fontweight="semibold")
        ax.tick_params(labelsize="xx-small")
    f.suptitle(
        "England: Covid-19 key data by region, log scale.", fontweight="bold"
    )
    f.savefig("KeyRegionalData.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    zoe = process_zoe(fetch_zoe_data())
    deaths = process_deaths_data(fetch_deaths_data())
    admissions, inpatients = make_healthcare_data()
    u60, o60, cases = process_cases_data(fetch_cases_data())

    # regions is the list of regions we want charts for
    regions_to_use = list(admissions.columns.unique())
    # hack to make England first
    regions_to_use.insert(
        0, regions_to_use.pop(regions_to_use.index("England"))
    )

    aggreg_df = aggreg(
        {
            "Zoe new infections": zoe,
            "Admissions": admissions,
            "Inpatients": inpatients,
            "Cases >60": o60,
            "Cases": cases,
            "Deaths": deaths,
        }
    )
    # uncomment if you want to save a csv table of all datasets
    # aggreg_df.to_csv('melted.csv')

    individual_charts(aggreg_df, regions_to_use)
    dashboard(aggreg_df, regions_to_use)

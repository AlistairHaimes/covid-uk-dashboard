#!/usr/bin/env python3
# coding: utf-8
"""
Builds dataframes on various covid-related metrics.
"""
from datetime import datetime, date, timedelta

import pandas as pd
import numpy as np
from uk_covid19 import Cov19API

from modules.utils import aggregate_dataframes


class ProcessedData:
    def dataframe(self):
        return self._process_data(self.fetch_raw_data())

    def fetch_raw_data(self) -> pd.DataFrame:
        raise NotImplementedError("overridden in subClass")

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("overridden in subClass")


class Zoe(ProcessedData):
    def fetch_raw_data(self) -> pd.DataFrame:
        zoe_path = "gcs://covid-public-data/csv/incidence_"
        zoe_dataframe = self._step_back(zoe_path)
        return zoe_dataframe

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        if not raw_data.empty:
            raw_data.drop(
                columns=raw_data.columns.difference(
                    ["date", "region", "covid_in_pop"]
                ),
                inplace=True,
            )
            zoe_dataframe = raw_data.pivot_table(
                index="date", columns="region"
            ).apply(np.int64)
            zoe_dataframe.columns = zoe_dataframe.columns.droplevel(0)
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

    @staticmethod
    def _step_back(zoe_path: str) -> pd.DataFrame:
        """Zoe's csv is normally dated about 5 days in the past, loop back from
        today's date to get the latest file."""
        today = date.today()
        for i in range(10):
            try:
                try_date = today - timedelta(days=i)
                try_file = zoe_path + (try_date.strftime("%Y%m%d")) + ".csv"
                zoe_dataframe = pd.read_csv(try_file)
                print(
                    f"downloaded Zoe data for {try_date.strftime('%d-%m-%Y')}"
                )
                return zoe_dataframe
            except ImportError as missing_gcsfs:
                # probably means gcsfs not installed
                raise ImportError(
                    "gcsfs package not installed, required"
                ) from missing_gcsfs
            except FileNotFoundError:
                print(f"no Zoe data for {try_date.strftime('%d-%m-%Y')}")
                continue
        raise FileNotFoundError("couldn't get Zoe data :(")


class GovCall:
    def timelogged_call_gov_api(
        self, callname: str, filters: list, structure: dict
    ) -> pd.DataFrame:
        """Helper function for api calls to PHE Covid19 API"""
        dataframe_from_api, timestamp = self.fetch_phe_data(filters, structure)
        parsed_timestamp = datetime.fromisoformat(timestamp.strip("Z"))
        print(
            f"got {callname} data to {parsed_timestamp.strftime('%d-%m-%Y')}"
        )
        return dataframe_from_api

    @staticmethod
    def fetch_phe_data(filters: list, structure: dict) -> pd.DataFrame:
        api = Cov19API(
            filters=filters,
            structure=structure,
        )
        return api.get_dataframe(), api.last_update


class Deaths(ProcessedData, GovCall):
    
    def _fetch_area(self, area_name: str) -> pd.DataFrame:
        areas = {
            "england": ["areaType=nation", "areaName=England"],
            "regions": ["areaType=region"],
        }
        deaths_query = {
            "date": "date",
            "region": "areaName",
            "deaths": "newDeaths28DaysByDeathDate"
        }
        out_df = self.timelogged_call_gov_api(
            callname=f"deaths ({area_name})",
            filters=areas[area_name],
            structure=deaths_query,
        )
        return out_df
    
    def fetch_raw_data(self):
        deaths_df = pd.concat(
            [self._fetch_area("regions"), self._fetch_area("england")],
            join="inner",
            ignore_index=True,
        )
        deaths_df.date = pd.to_datetime(deaths_df.date)
        return deaths_df

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        deaths_df = raw_data
        #england = deaths_df.pivot_table(index="date", columns="region").sum(axis=1)
        midlands = self._concatenate_regions(
            deaths_df,
            input_regions=["East Midlands", "West Midlands"],
            output_label="Midlands",
        )
        ney = self._concatenate_regions(
            deaths_df,
            input_regions=["North East", "Yorkshire and The Humber"],
            output_label="North East and Yorkshire",
        )
        deaths_df = deaths_df.set_index("date").append([ney, midlands])
        deaths_df = deaths_df.reset_index()
        deaths_df = deaths_df.pivot_table(index="date", columns="region")
        deaths_df.columns = deaths_df.columns.droplevel(0)
        # exclude final 3 days as figures will be updated
        deaths_df = deaths_df.iloc[:-3]
        return deaths_df

    @staticmethod
    def _concatenate_regions(
        big_df: pd.DataFrame, input_regions: list, output_label: str
    ) -> pd.DataFrame:
        out_dataframe = (
            pd.concat(
                [
                    big_df[big_df.region == input_region]
                    for input_region in input_regions
                ]
            )
            .groupby("date")
            .sum()
        )
        out_dataframe["region"] = output_label
        return out_dataframe

    
class Healthcare(ProcessedData, GovCall):
    def __init__(self):
        # so I don't hit the db repeatedly
        self._dataframe = self.dataframe()
        # new - tidy up
        self._default_area_query = {
            "england": ["areaType=nation", "areaName=England"],
            "regions": ["areaType=nhsRegion"],
        }

    def metric(self, metric: str) -> pd.DataFrame:
        """Choose from `admissions`, `inpatients` or `icu`"""
        return self._dataframe.pivot_table(
            index="date", columns="region", values=metric
        )

    def _fetch_area(self, area_name: str) -> pd.DataFrame:
        areas = {
            "england": ["areaType=nation", "areaName=England"],
            "regions": ["areaType=nhsRegion"],
        }
        healthcare_query = {
            "date": "date",
            "region": "areaName",
            "admissions": "newAdmissions",
            "inpatients": "hospitalCases",  # inpatients
            "icu": "covidOccupiedMVBeds",  # kinda ICU
        }
        out_df = self.timelogged_call_gov_api(
            callname=f"healthcare ({area_name})",
            filters=areas[area_name],
            structure=healthcare_query,
        )
        return out_df

    def fetch_raw_data(self):
        healthcare_dfs = pd.concat(
            [self._fetch_area("regions"), self._fetch_area("england")],
            join="inner",
            ignore_index=True,
        )
        return healthcare_dfs

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        healthcare_dfs = raw_data
        healthcare_dfs.date = pd.to_datetime(healthcare_dfs.date)
        return healthcare_dfs


class Cases(ProcessedData, GovCall):
    def __init__(self):
        # so I don't hit the db repeatedly
        self._dataframe = self.dataframe()

    def metric(self, metric: str) -> pd.DataFrame:
        """Choose from `u60`, `o60` or `all_ages`"""
        cases_dataframe = self._dataframe
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
        all_ages_df = u60_df + o60_df
        choose_df = {"u60": u60_df, "o60": o60_df, "all_ages": all_ages_df}
        return choose_df[metric]

    def fetch_raw_data(self):
        cases_query = {
            "metric": "newCasesBySpecimenDateAgeDemographics",
            "region": "areaName",
            "date": "date",
        }
        cases_dataframe = self.timelogged_call_gov_api(
            callname="cases",
            filters=["areaType=region"],
            structure=cases_query,
        )
        return cases_dataframe

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        cases_dataframe = raw_data
        cases_dataframe.date = pd.to_datetime(cases_dataframe.date)
        cases_dataframe = self._expand_from_explode(
            cases_dataframe, ["age", "cases"]
        )
        england = cases_dataframe.groupby(["date", "age"]).sum()
        england["region"] = "England"
        england = england.reset_index().set_index("date")
        cases_dataframe = pd.concat([cases_dataframe, england])
        midlands = self._concatenate_regions(
            cases_dataframe,
            input_regions=["East Midlands", "West Midlands"],
            output_label="Midlands",
        )
        ney = self._concatenate_regions(
            cases_dataframe,
            ["North East", "Yorkshire and The Humber"],
            output_label="North East and Yorkshire",
        )
        return pd.concat([cases_dataframe, ney, midlands])

    @staticmethod
    def _expand_from_explode(
        raw_df: pd.DataFrame, metrics: list
    ) -> pd.DataFrame:
        """Expands the 'metric` dictionary column into separate columns"""
        exploded_df = raw_df.explode("metric").reset_index()
        processed_df = (
            exploded_df.join(pd.json_normalize(exploded_df.metric)[metrics])
            .set_index("date")
            .drop(columns=["index", "metric"])
        )
        return processed_df

    @staticmethod
    def _concatenate_regions(
        in_df: pd.DataFrame, input_regions: list, output_label: str
    ) -> pd.DataFrame:
        out_df = (
            pd.concat(
                [
                    in_df[in_df.region == input_region]
                    for input_region in input_regions
                ]
            )
            .reset_index()
            .groupby(["date", "age"])
            .sum()
            .reset_index()
            .set_index("date")
        )
        out_df["region"] = output_label
        return out_df


def make_default_dataframes():
    zoe = Zoe().dataframe()
    deaths = Deaths().dataframe()
    healthcare = Healthcare()
    admissions = healthcare.metric("admissions")
    inpatients = healthcare.metric("inpatients")
    icu = healthcare.metric("icu")
    cases = Cases()
    o60 = cases.metric("o60")
    cases = cases.metric("all_ages")

    # regions is the list of regions we want charts for
    regions_to_use = list(admissions.columns.unique())
    # hack to make England first
    regions_to_use.insert(
        0, regions_to_use.pop(regions_to_use.index("England"))
    )

    aggreg_df = aggregate_dataframes(
        {
            "Zoe new infections": zoe,
            "Admissions": admissions,
            "Inpatients": inpatients,
            "Cases >60": o60,
            "Cases": cases,
            "Deaths": deaths,
            "MechVent inpatients": icu,
        }
    )
    return aggreg_df, regions_to_use

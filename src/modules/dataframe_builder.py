#!/usr/bin/env python3
# coding: utf-8
"""
Builds dataframes on various covid-related metrics.
"""
from datetime import datetime, date, timedelta

import pandas as pd
import numpy as np
from uk_covid19 import Cov19API


class ProcessedData:
    def dataframe(self):
        return self._process_data(self.fetch_raw_data())

    def fetch_raw_data(self) -> pd.DataFrame:
        pass

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        pass


class Zoe(ProcessedData):
    def fetch_raw_data(self) -> pd.DataFrame:
        """Zoe's csv is normally dated about 5 days in the past, loop back from
        today's date to get the latest file."""
        zoe_path = "gcs://covid-public-data/csv/incidence_"
        zoe_dataframe = self._step_back(zoe_path)
        return zoe_dataframe

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        if not raw_data.empty:
            raw_data.drop(
                raw_data.columns.difference(
                    ["date", "region", "covid_in_pop"]
                ),
                1,
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
        return None


class GovCall:  # pylint: disable=too-few-public-methods
    def call_gov_api(
        self, callname: str, filters: list, structure: dict
    ) -> pd.DataFrame:
        """Helper function for api calls to PHE Covid19 API"""
        dataframe_from_api, timestamp = self._fetch_phe_data(
            filters, structure
        )
        parsed_timestamp = datetime.fromisoformat(timestamp.strip("Z"))
        print(
            f"got {callname} data to {parsed_timestamp.strftime('%d-%m-%Y')}"
        )
        return dataframe_from_api

    @staticmethod
    def _fetch_phe_data(filters: list, structure: dict) -> pd.DataFrame:
        api = Cov19API(
            filters=filters,
            structure=structure,
        )
        return api.get_dataframe(), api.last_update


class Deaths(ProcessedData, GovCall):
    def fetch_raw_data(self):
        deaths_query = {
            "date": "date",
            "region": "areaName",
            "deaths": "newDeaths28DaysByDeathDate",
        }
        deaths_df = self.call_gov_api(
            callname="deaths",
            filters=["areaType=region"],
            structure=deaths_query,
        )
        deaths_df.date = pd.to_datetime(deaths_df.date)
        return deaths_df

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        deaths_df = raw_data
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
        england = deaths_df.groupby("date").sum().reset_index()
        england["region"] = "England"
        deaths_df = pd.concat([deaths_df.reset_index(), england])
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
        out_df = self.call_gov_api(
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
        cases_dataframe = self.call_gov_api(
            callname="cases",
            filters=["areaType=region"],
            structure=cases_query,
        )
        return cases_dataframe.explode("metric")

    def _process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        cases_dataframe = raw_data
        cases_dataframe = self._expand_from_explode(
            cases_dataframe, ["age", "cases"]
        )
        cases_dataframe.date = pd.to_datetime(cases_dataframe.date)
        cases_dataframe = cases_dataframe.set_index("date")
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
    def _expand_from_explode(exploded_df, metrics):
        for metric in metrics:
            exploded_df[metric] = exploded_df["metric"].apply(
                lambda x: x[metric]  # pylint: disable=cell-var-from-loop
            )
        return exploded_df

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

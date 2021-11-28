#!/usr/bin/env python3
# coding: utf-8

from datetime import datetime, date, timedelta # for zoe string

import pandas as pd
import matplotlib as mpl # for formatting log y axis
from matplotlib.dates import MonthLocator # for formatting x axis 
import matplotlib.dates as mdates # for formatting x axis
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from uk_covid19 import Cov19API


# slice to 1st wave, say 1st March
start_w1 = '2020-03-01'

# chart preferences
sns.set_style("ticks") # darkgrid looks nice too
sns.set_color_codes("dark")
mpl.use('Agg') # so can save without displaying


# Zoe data
def fetch_zoe_data() -> pd.DataFrame:
    """# Zoe's csv is normally dated about 5 days in the past, loop back from
    today's date to get the latest file.
    """
    today = date.today()
    zoe_path='gcs://covid-public-data/csv/incidence_'
    for i in range(10):
        try:
            tryDate = (today - timedelta(days=i))
            tryFile = zoe_path + (tryDate.strftime("%Y%m%d")) + '.csv'
            df = pd.read_csv(tryFile)
            # uncomment to save current file
            # df.to_csv('zoe.csv')
            print(f"downloaded Zoe data for {tryDate.strftime('%d-%m-%Y')}")
            return df
        except ImportError:
            # probably means gcsfs not installed
            raise ImportError("gcsfs package not installed, required")
        except FileNotFoundError:
            print(f"no Zoe data for {tryDate.strftime('%d-%m-%Y')}")

def process_Zoe(raw_zoe: pd.DataFrame) -> pd.DataFrame:
    if not raw_zoe.empty:
        raw_zoe.drop(raw_zoe.columns.difference(['date', 'region', 
            'covid_in_pop']), 1, inplace=True)
        zoe = raw_zoe.pivot_table(
            index='date',
            columns='region').apply(np.int64)
        zoe.columns=zoe.columns.droplevel(0)
        to_drop = ['Northern Ireland', 'Scotland', 'UK', 'Wales']       
        zoe = zoe.drop(to_drop, axis=1)
        zoe.index=pd.to_datetime(zoe.index, dayfirst=True)
        zoe['Midlands'] = zoe['West Midlands'] + zoe['East Midlands']
        zoe['North East and Yorkshire'] = (zoe['North East'] + 
            zoe['Yorkshire and The Humber'])
        return zoe
    else:
        raise ValueError("no Zoe DataFrame, presume problem at GCS")

zoe = process_Zoe( fetch_zoe_data() )


def call_gov_api(callname: str, filters: list, structure: dict) -> pd.DataFrame:
    '''helper function for api call to UK govt covid19 api'''
    api = Cov19API(
        filters=filters, 
        structure=structure,
        )
    df = api.get_dataframe()
    timestamp = api.last_update
    parsed_timestamp = datetime.fromisoformat(timestamp.strip("Z"))
    print(f"got {callname} data to {parsed_timestamp.strftime('%d-%m-%Y')}")
    # uncomment below if you want a csv
    # df.to_csv(f'{areaName}Healthcare.csv')
    return df

# deaths
def fetch_deaths_data():
    deaths_query = {
        'date':'date',
        "region": "areaName",
        "deaths":"newDeaths28DaysByDeathDate",
        }
    deaths = call_gov_api(callname='deaths', filters=['areaType=region'], 
        structure=deaths_query)
    deaths.date = pd.to_datetime(deaths.date)
    return deaths

def process_deaths_data(deaths: pd.DataFrame) -> pd.DataFrame:
    ney = deaths[(deaths.region=='North East') | (deaths.region=='Yorkshire and The Humber')].groupby('date').sum()
    midlands = deaths[(deaths.region=='East Midlands') | (deaths.region=='West Midlands')].groupby('date').sum()
    to_add1, to_add2 = pd.DataFrame(ney), pd.DataFrame(midlands)
    to_add1['region'], to_add2['region'] ='North East and Yorkshire', 'Midlands'
    deaths = deaths.set_index('date').append([to_add1, to_add2])
    england = deaths.groupby('date').sum().reset_index()
    england['region'] = 'England'
    deaths = pd.concat([deaths.reset_index(), england])
    deaths = deaths.pivot_table(index='date', columns='region')
    deaths.columns = deaths.columns.droplevel(0)
    # exclude final 3 days as figures will be updated
    deaths = deaths.iloc[:-3]
    return deaths

deaths = process_deaths_data( fetch_deaths_data() )


# Hospital data
def fetch_healthcare_data(areaName: str) -> pd.DataFrame:
    areas = {
        'england': ['areaType=nation', 'areaName=England'],
        'regions': ['areaType=nhsRegion']
        }
    healthcare_query = {
        "date":"date",
        "region": "areaName",
        "newAdmissions": "newAdmissions",
        "hospitalCases": "hospitalCases", # inpatients
        #'covidOccupiedMVBeds': 'covidOccupiedMVBeds', # kinda ICU
        }
    return call_gov_api(callname=f'healthcare ({areaName})', 
        filters=areas[areaName], structure=healthcare_query)


def make_healthcare_data():
    healthcare_dfs = pd.concat(
        [fetch_healthcare_data('regions'), fetch_healthcare_data('england')], 
        join='inner', ignore_index=True)
    healthcare_dfs.date = pd.to_datetime(healthcare_dfs.date)
    admissions = healthcare_dfs.pivot_table(
        index='date', columns='region', values='newAdmissions')
    inpatients = healthcare_dfs.pivot_table(
        index='date', columns='region', values='hospitalCases')
    return admissions, inpatients

admissions, inpatients = make_healthcare_data()

# if you want to save a csv of each dataset, uncomment the below:
# admissions.to_csv('admissions.csv')
# inpatients.to_csv('inpatients.csv')


# Cases
def fetch_cases_data():
    cases_query = {
            'metric':'newCasesBySpecimenDateAgeDemographics',
            'region': 'areaName',
            'date':'date',
            }
    cases_dataframe = call_gov_api(callname='cases', filters=['areaType=region'], 
        structure=cases_query)
    return cases_dataframe

def process_cases_data(cases_dataframe: pd.DataFrame) -> pd.DataFrame:
    cases_dataframe=cases_dataframe.explode('metric')
    cases_dataframe['age']=cases_dataframe['metric'].apply(lambda x:x['age'])
    cases_dataframe['cases']=cases_dataframe['metric'].apply(lambda x:x['cases'])
    cases_dataframe.date = pd.to_datetime(cases_dataframe.date)
    cases_dataframe = cases_dataframe.set_index('date')
    england = cases_dataframe.groupby(['date', 'age']).sum()
    england['region'] = 'England'
    england = england.reset_index().set_index('date')
    cases_dataframe = pd.concat([cases_dataframe, england])
    midlands = pd.concat([cases_dataframe[cases_dataframe.region=="East Midlands"], 
        cases_dataframe[cases_dataframe.region=="West Midlands"]])
    midlands = midlands.reset_index().groupby(
        ['date', 'age']).sum().reset_index().set_index('date')
    midlands['region'] = 'Midlands'
    ney = pd.concat([cases_dataframe[cases_dataframe.region=="North East"], 
        cases_dataframe[cases_dataframe.region=="Yorkshire and The Humber"]])
    ney = ney.reset_index().groupby(
        ['date', 'age']).sum().reset_index().set_index('date')
    ney['region']='North East and Yorkshire'
    cases_dataframe = pd.concat([cases_dataframe, ney, midlands])
    to_drop = ['North East', 'Yorkshire and The Humber', 'East Midlands', 'West Midlands']
    
    for to_drop_region in to_drop:
        cases_dataframe = cases_dataframe[cases_dataframe.region!=to_drop_region]
    u60 = cases_dataframe[cases_dataframe.age=='00_59'].pivot_table(index='date', columns='region').cases
    o60 = cases_dataframe[cases_dataframe.age=='60+'].pivot_table(index='date', columns='region').cases
    cases = u60 + o60
    return u60, o60, cases

u60, o60, cases = process_cases_data( fetch_cases_data() )


# create charts

regions = list(admissions.columns.unique())
regions.remove('England')
regions.insert(0, 'England')

# make a big dataFrame with all the ones to plot in
to_plot = pd.concat([zoe, admissions, inpatients, o60, cases, deaths],
    keys=['zoe', 'admissions', 'inpatients', 'cases 60+', 'cases', 'deaths'],
    axis=1,
    join='outer')
# uncomment if you want to save a csv table of all datasets 
# to_plot.to_csv('melted.csv')


def format_ax(ax: plt.subplot, region: str) -> plt.subplot:
    """common formatting for all charts"""
    legendLabels = ['Zoe new infections','Admissions', 'Inpatients', 
        'Cases >60', 'Cases', 'Deaths']
    yticksLog=[1,10,100,1000,10000] # for log y-scale
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles=handles, 
        labels=legendLabels, 
        title='', 
        loc='upper left',
        facecolor='white')
    ax.margins(x=0.01) #0.05 is default if not specified
    ax.set_yscale("log")
    ax.xaxis.set_label_text('') # obviously date
    ax.set_yticks(yticksLog)
    if (region == 'England'):
        ax.set_ylim(bottom=1)
    else:
        ax.set_ylim(bottom=1, top=20000)
    ax.yaxis.tick_right()
    ax.yaxis.set_label_text(label='')
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.xaxis.set_major_locator(MonthLocator())
    ax.get_xaxis().set_major_formatter(mdates.DateFormatter("%b"))
    return ax


def individual_charts():
    for counter, region in enumerate(regions):
        fig, ax = plt.subplots(figsize=(12, 7))
        sns.lineplot(
            data=to_plot[~(to_plot.index < start_w1)].swaplevel(axis=1)[region].reset_index().melt(id_vars=['date']), 
            x="date",
            y="value",
            ax=ax, 
            linewidth=0.5,
            hue="variable")
        sns.lineplot(
            data=to_plot.rolling(window=7).mean()[~(to_plot.index < start_w1)].swaplevel(axis=1)[region].reset_index().melt(id_vars=['date']),
            x="date",
            y="value",
            ax=ax,
            linewidth=2.0,
            hue="variable",
            #legend=None
        )
        mpl.rcParams['legend.fontsize'] = 'medium'
        ax = format_ax(ax, region)
        ax.tick_params(labelsize='large')
        fig.suptitle(f'{region}: Covid-19 key data, log scale', y=0.93, fontweight='bold')
        fig.savefig(region.replace(" ", "") + "KeyData.png", dpi=200)
        plt.close()


### regional dashboard
def dashboard():
    f = plt.figure(constrained_layout=True, figsize=(12, 10), dpi=300) # A4, landscape format
    gs = f.add_gridspec(4,2)
    mpl.rcParams['legend.fontsize'] = 'xx-small'
    for counter, region in enumerate(regions):
        # create ax in the right place 
        ax = f.add_subplot(gs[(counter%4, counter//4)])
        sns.lineplot(
            data=to_plot[~(to_plot.index < start_w1)].swaplevel(axis=1)[region].reset_index().melt(id_vars=['date']), 
            x="date",
            y="value",
            ax=ax, 
            linewidth=0.15,
            hue="variable")
        sns.lineplot(
            data=to_plot.rolling(window=7).mean()[~(to_plot.index < start_w1)].swaplevel(axis=1)[region].reset_index().melt(id_vars=['date']),
            x="date",
            y="value",
            ax=ax,
            linewidth=1.0,
            hue="variable",
            #legend=None,
            )
        ax = format_ax(ax, region)
        ax.set_title(label=region, fontsize='small', fontweight='semibold')
        ax.tick_params(labelsize='xx-small')
    f.suptitle('England: Covid-19 key data by region, log scale.', fontweight='bold')
    f.savefig("KeyRegionalData.png", dpi=300)
    plt.close('all')


if __name__ == '__main__':
    individual_charts()
    dashboard()
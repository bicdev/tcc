import os
import zipfile
import requests
import shutil

import pandas as pd
import geopandas as gpd


MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(MODULE_DIR)
CACHE_DIR = os.path.join(PROJECT_ROOT, 'cached data')

print('data_loader imported')
EXPERIMENT_CONFIG = {}

def download_file(url, destination):
    with requests.get(url) as response:
        response.raise_for_status()
        with open(destination, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)

def load_subway():
    url = "https://raw.githubusercontent.com/bicdev/tcc/refs/heads/main/cached%20data/MTA_Subway_Stations_20250217.csv"
    filename = os.path.join(CACHE_DIR, "subway_rawdata.csv")

    if not os.path.exists(filename):
        print('downloading subway data')
        download_file(url, filename)
    df_subway = pd.read_csv(filename)
    return df_subway

def load_census():
    url = "https://raw.githubusercontent.com/bicdev/tcc/7c10dd978fcac26e6d6eee38629a25e159697faa/cached%20data/race_comparison_manhattan.geojson"
    filename = os.path.join(CACHE_DIR, "census_rawdata.geojson")

    if not os.path.exists(filename):
        print('downloading census data')
        download_file(url, filename)
    df_census = gpd.read_file(filename)
    return df_census

def load_citibike(year: str, month: str):
    url = "https://s3.amazonaws.com/tripdata/2020-citibike-tripdata.zip"
    bigzip = os.path.join(CACHE_DIR, f'{year}-citibike-tripdata.zip')

    if not os.path.exists(bigzip):
        print('downloading citibike data')
        download_file(url, bigzip)
        with zipfile.ZipFile(bigzip, 'r') as zip_ref:
            zip_ref.extractall('tcc\cached data\\')

    smallzip = os.path.join(CACHE_DIR, f'{bigzip[:-4]}\{year}{month}-citibike-tripdata.zip')
    if not os.path.exists(smallzip):
        with zipfile.ZipFile(smallzip, 'r') as zip_ref:
            zip_ref.extractall('tcc\cached data\\')


def aggregate_citibike_datasets(year: str, months: list):
    aggregated_dataset = pd.DataFrame()
    for month in months:
        aggregated_dataset = pd.concat([aggregated_dataset, load_citibike(year, month)])
    return aggregated_dataset


def load_datasets():
    year = EXPERIMENT_CONFIG['year']
    month = EXPERIMENT_CONFIG['month']
    if type('month') == list:
        citibike_dataset = aggregate_citibike_datasets(year, month)
    else:
        citibike_dataset = load_citibike()
    subway_dataset = load_subway()
    census_dataset = load_census()
    pass


def load(config: dict):
    EXPERIMENT_CONFIG = config
    load_datasets()
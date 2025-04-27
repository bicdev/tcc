import os
import zipfile
import geopandas as gpd
import pandas as pd
import geodatasets # type: ignore
from urllib.request import urlretrieve

def acquire_jan2020():
  url = "https://s3.amazonaws.com/tripdata/2020-citibike-tripdata.zip"
  output_filename = "content/" + url.split('/')[-1]
  if not os.path.exists('content/2020-citibike-tripdata.zip'):
    urlretrieve(url, output_filename)
    print("Raw data downloaded")
  
  with zipfile.ZipFile("content/2020-citibike-tripdata.zip", 'r') as zip_ref:
    zip_ref.extractall("content/")
    print("Unzipping year's file")
  
  with zipfile.ZipFile('content/2020-citibike-tripdata/202001-citibike-tripdata.zip', 'r') as zip_ref:
    zip_ref.extractall("content/")
    print("Unzipping month's file")

    
def acquire_raw_data(url):
  url = "https://s3.amazonaws.com/tripdata/2020-citibike-tripdata.zip"
  output_filename = "content/" + url.split('/')[-1]
  if not os.path.exists('content/2020-citibike-tripdata.zip'):
    urlretrieve(url, output_filename)
    print("Raw data downloaded")

  with zipfile.ZipFile("content/2020-citibike-tripdata.zip", 'r') as zip_ref:
    zip_ref.extractall("content/")
    print("Unzipping year's file")

  with zipfile.ZipFile('content/2020-citibike-tripdata/202001-citibike-tripdata.zip', 'r') as zip_ref:
    zip_ref.extractall("content/")
    print("Unzipping month's file")

def get_manhattan_gdf() -> gpd.GeoDataFrame:
  path = geodatasets.get_path("nybb")
  df_nybb = gpd.read_file(path)
  df_nybb = df_nybb.to_crs(epsg=4326)
  manhattan_polygon = df_nybb.iloc[3]['geometry']
  manhattan_gdf = gpd.GeoDataFrame(geometry=[manhattan_polygon])
  manhattan_gdf.crs = "EPSG:4326"
  return manhattan_gdf

def get_race_census(self) -> gpd.geodataframe:
  if not os.path.exists('content/race_comparison_manhattan.geojson'):
    url = '''https://raw.githubusercontent.com/bicdev/tcc/7c10dd978fcac26e6d6eee38629a25e159697faa/cached%20data/race_comparison_manhattan.geojson'''
    print("downloading cached data from github")
    urlretrieve(url, 'content/race_comparison_manhattan.geojson')
  return gpd.read_file('content/race_comparison_manhattan.geojson')

def get_tracts_racialized(df_race_census: pd.DataFrame, slice_size: int) -> pd.DataFrame:
  df_racial_delta = df_race_census[[
    'NAME', 
    'geometry',
    'Total:  >  Population of one race:  >  White alone',
    'Total:  >  Population of one race:  >  Black or African American alone',
    'Total:  >  Population of one race:  >  American Indian and Alaska Native alone',
    'Total:  >  Population of one race:  >  Asian alone',
    'Total:  >  Population of one race:  >  Native Hawaiian and Other Pacific Islander alone',
    'Total:  >  Population of one race:  >  Some Other Race alone'
  ]]

  df_racial_delta['Racial Ratio'] = pd.Series()
  df_racial_delta['Racial Label'] = pd.Series()
  
  for i, row in df_racial_delta.iterrows():
    whites = row['Total:  >  Population of one race:  >  White alone']
    non_whites = (
      row['Total:  >  Population of one race:  >  Black or African American alone'] +
      row['Total:  >  Population of one race:  >  American Indian and Alaska Native alone'] +
      row['Total:  >  Population of one race:  >  Asian alone'] +
      row['Total:  >  Population of one race:  >  Native Hawaiian and Other Pacific Islander alone'] +
      row['Total:  >  Population of one race:  >  Some Other Race alone']
    )

    if whites > non_whites:
      label = "Whites"
    elif whites < non_whites:
      label = "Non-Whites"
    else:
      label = "None"

    if whites == 0.0 or non_whites == 0.0:
      ratio = 0
      label = "None"
    else:
      ratio = min(whites, non_whites) / max(whites, non_whites)
    
    df_racial_delta.loc[i,'Racial Ratio'] = ratio
    df_racial_delta.loc[i,'Racial Label'] = label

  df_racial_delta.sort_values(by='Racial Ratio', ascending=False, inplace=True)
  df_racial_delta.reset_index(inplace=True)
  ids_to_drop = df_racial_delta[df_racial_delta['Racial Label'] == "None"].index
  df_racial_delta.drop(ids_to_drop, inplace=True)

  gdf_top_slice_racialized = gpd.GeoDataFrame(
    df_racial_delta,
    geometry=df_racial_delta['geometry'],
    crs=4326)

  return gdf_top_slice_racialized

def acquire_data(year:str, month:str, tracts_amount:int, manhattan_only = True):


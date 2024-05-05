import os
import datetime as dt

import requests

import tqdm
import pandas as pd

from multiprocessing import Pool

import databricks.koalas as ks
# Setting variable to java path so koalas knows where to look for it 
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-11\bin'
ks.set_option('compute.default_index_type', 'distributed-sequence') 


def _download_taxi_files(date_fname):
    # Get date and fname from input
    (date, fname) = date_fname
    yellow_taxis_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:02d}-{month:02d}.parquet'
    
    # Only download file if it doesn't already exist
    if os.path.isfile(fname): return
    # Download file and write it's content
    response = requests.get(yellow_taxis_url.format(year=date.year, month=date.month))
    with open(fname, 'wb') as f: f.write(response.content)


def download_taxi_files(date_range, output_folder):
    # Create output_folder if it doesn't exist
    if not os.path.isdir(output_folder): os.mkdir(output_folder)

    # Use a Pool of workers to download files in parallel
    date_output_name = [(date, f'{output_folder}/yellow_taxi_tripdata_{date.year:02d}-{date.month:02d}.parquet') for date in date_range]
    with Pool() as p:
        pbar = tqdm.tqdm(total=len(date_range))
        for _ in p.imap_unordered(_download_taxi_files, date_output_name): pbar.update()
    return date_output_name


def treat_file(fname, outname):
    ks_df= ks.read_parquet(fname, index_col='index')
    # Rename columns to lower case
    ks_df.columns = ks_df.columns.str.lower()
    # write files to new parquet files
    ks_df.to_parquet(outname, index_col='index')


def main():
    # Which years we wan't to fetch the data from
    years = range(2009, 2010)
    date_range = [dt.datetime(year, month, 1) for year in years for month in range(1, 13)]

    # To which directory do we wish to save the data in 
    split = '/' if '/' in __file__ else '\\'
    output_folder = '/'.join(__file__.split(split)[:-2]) + '/datasets'

    # Download taxi files
    download_taxi_files(date_range, output_folder)

    # Renamimg some coolumns
    treat_file(f'{output_folder}/yellow_taxi_tripdata_', f'{output_folder}/ks_yellow_taxi_tripdata_')


if __name__ == '__main__':
    main()
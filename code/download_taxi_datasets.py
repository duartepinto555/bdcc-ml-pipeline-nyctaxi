import os
import tqdm
import requests
import pandas as pd
import datetime as dt

from multiprocessing import Pool


import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


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


def treat_files(fname, outname):
    cluster = LocalCluster(n_workers=4, threads_per_worker=3, memory_limit='30GiB')
    
    with Client(cluster):
        # Renaming columns to matchh article ones
        renaming_cols = {
            'passenger_count': 'Passenger_Count',
            'fare_amount': 'Fare_Amt',
            'tip_amount': 'Tip_Amt',
            # The 2 renamings bellow, don't show the exact LAT/LON, 
            # but we only want to benchmark, so they're not entirely relevant...
            # We'll later create the respective Lat coordinates by copying the values of this lon ones
            'PULocationID': 'Start_Lon',
            'DOLocationID': 'End_Lon',

            'tolls_amount': 'Tolls_Amt',
            'total_amount': 'Total_Amt',
        }

        # Copying the dtype dict from the article
        dtype_dict = {
            'Passenger_Count': 'int64', 
            'Start_Lon': 'float64', 
            'Start_Lat': 'float64',
            'End_Lon': 'float64', 
            'End_Lat': 'float64', 
            'Fare_Amt': 'float64', 
            'Tip_Amt': 'float64', 
            'Tolls_Amt': 'float64',
            'Total_Amt': 'float64'
        }
        # Convert Article dtype_dict to new one
        dtype_dict = {col: dtype_dict[renaming_cols[col]] for col in renaming_cols.keys()}
        df = dd.read_parquet(fname, dtype=dtype_dict)

        # Drop all other columns not relevant (to us)
        df = df.drop(columns=df.columns.difference(dtype_dict.keys()))

        # Renaming columns to match old format
        df = df.rename(columns=renaming_cols)

        # Create the Lat/Lon columns
        df['Start_Lat'] = df['Start_Lon'].copy()
        df['End_Lat'] = df['End_Lon'].copy()

        # Make columns all lower
        df.columns = df.columns.str.lower()

        # Save dataframe as parquet file in output folder
        df.to_parquet(outname)


def main():
    # Which years we wan't to fetch the data from
    years = range(2011, 2016)
    date_range = [dt.datetime(year, month, 1) for year in years for month in range(1, 13)]

    # To which directory do we wish to save the data in 
    split = '/' if '/' in __file__ else '\\'
    output_folder = '/'.join(__file__.split(split)[:-2]) + '/datasets/taxi_csv'

    # Creating folders to be able to download data corectly
    os.makedirs(output_folder, exist_ok=True)

    # Download taxi files
    download_taxi_files(date_range, output_folder)

    # Treating files
    output_folder_2 = '/'.join(output_folder.split('/')[:-1]) + '/ks_taxi_parquet'
    os.makedirs(output_folder_2, exist_ok=True)
    treat_files(output_folder, output_folder_2)
    return output_folder_2


if __name__ == '__main__':
    main()
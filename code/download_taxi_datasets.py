import os
import tqdm
import requests
import pandas as pd
import datetime as dt
from math import sin, cos, sqrt, atan2, radians

from multiprocessing import Pool

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

def calculate_distance(lat_1, lon_1, lat_2, lon_2):
    
    R = 6373.0
    lat_1 = radians(lat_1)
    lon_1 = radians(lon_1)
    lat_2 = radians(lat_2)
    lon_2 = radians(lon_2)

    dlon = lon_2 - lon_1
    dlat = lat_2 - lat_1

    a = sin(dlat / 2)**2 + cos(lat_1) * cos(lat_2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c

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
    if not os.path.isdir(output_folder): os.makedirs(output_folder, exist_ok=True)

    # Use a Pool of workers to download files in parallel
    date_output_name = [(date, f'{output_folder}/yellow_taxi_tripdata_{date.year:02d}-{date.month:02d}.parquet') for date in date_range]
    with Pool() as p:
        pbar = tqdm.tqdm(total=len(date_range))
        for _ in p.imap_unordered(_download_taxi_files, date_output_name): pbar.update()
    return date_output_name


def treat_files(fname, outname, num_partitions=24, partition_size=None):
    # If files already exist in output folder, than data treatment has already been done
    if len(os.listdir(outname)) > 3: 
        return

    # cluster = LocalCluster(n_workers=4, threads_per_worker=3, memory_limit='30GiB')
    cluster = LocalCluster(n_workers=1, threads_per_worker=3, memory_limit='20GiB')
    
    with Client(cluster) as client:
        print(f'Initialized Dask Cluster. Visualize dashboard at {client.dashboard_link}')
        # Renaming columns to match article ones
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
            'trip_distance': 'distance',
            'tpep_pickup_datetime': 'Pickup_Datetime'
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
            'Total_Amt': 'float64',
            'distance': 'float64',
            'Pickup_Datetime': 'datetime'
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

        # Model cannot run with a single unique value.
        # df = df[df['fare_amt'].duplicated(keep=False)]
        # df = df.drop_duplicates(subset=['fare_amt'])

        # Repartition the dataframe (create more (or less) files)
        # Give priority to partition_size, if it doesn't exist, use num_partitions
        df = df.repartition(partition_size=partition_size) if partition_size is not None else df.repartition(npartitions=num_partitions)
        
        # Save dataframe as parquet file in output folder
        df.to_parquet(outname)

        client.shutdown()


def main(years=range(2011, 2012), n_partitions=8, sample_size=6, partition_size=None):
    # Which years we wan't to fetch the data from
    date_range = [dt.datetime(year, month, 1) for year in years for month in range(1, 13)]
    date_range = date_range[:sample_size]     # Use only the first 6 months, this makes the script possible to execute using only pandas

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
    treat_files(output_folder, output_folder_2, num_partitions=n_partitions, partition_size=partition_size)
    return output_folder_2


if __name__ == '__main__':
    main()
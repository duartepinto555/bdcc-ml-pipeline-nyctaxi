import requests
import tqdm
import os
import datetime as dt
from multiprocessing import Pool


def download_taxi_files(date_fname):
    (date, fname) = date_fname
    yellow_taxis_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:02d}-{month:02d}.parquet'
    
    # Only download file if it doesn't already exist
    if os.path.isfile(fname): return
    response = requests.get(yellow_taxis_url.format(year=date.year, month=date.month))
    with open(fname, 'wb') as f:
        f.write(response.content)
    return


def main(date_range, output_folder):
    # Use a Pool of workers to download files in parallel
    date_output_name = [(date, f'{output_folder}/yellow_taxi_tripdata_{date.year:02d}-{date.month:02d}.parquet') for date in date_range]
    with Pool() as p:
        pbar = tqdm.tqdm(total=len(date_range))
        for _ in p.imap_unordered(download_taxi_files, date_output_name): pbar.update()


if __name__ == '__main__':
    years = range(2009, 2013)
    date_range = [dt.datetime(year, month, 1) for year in years for month in range(1, 12)]
    output_folder = '/'.join(__file__.split('/')[:-2]) + '/datasets'
    main(date_range, output_folder)
import datetime as dt
import download_taxi_datasets


def main():    
    # Which years we wan't to fetch the data from
    years = range(2009, 2010)
    date_range = [dt.datetime(year, month, 1) for year in years for month in range(1, 13)]

    # To which directory do we wish to save the data in 
    output_folder = '/'.join(__file__.split('/')[:-2]) + '/datasets'
    download_taxi_datasets.download_taxi_files(date_range, output_folder)


if __name__ == '__main__':
    main()
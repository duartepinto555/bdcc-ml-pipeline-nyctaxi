import tqdm
import datetime as dt
import download_taxi_datasets
from benchmarks.benchmark_class import Benchmark


def main():    
    # Which years we wan't to fetch the data from
    years = range(2009, 2010)
    date_range = [dt.datetime(year, month, 1) for year in years for month in range(1, 13)]

    # To which directory do we wish to save the data in 
    output_folder = '/'.join(__file__.split('/')[:-2]) if '/' in __file__ else '/'.join(__file__.split('\\')[:-2]) + '/datasets'
    # download_taxi_datasets.download_taxi_files(date_range, output_folder)

    # Run benchmarks for all engines
    engine_configs = [
        {'engine': 'pandas', 'df_type': 'pandas'},
        {'engine': 'modin', 'df_type': 'pandas'},
        {'engine': 'dask', 'df_type': 'pandas'},
        {'engine': 'joblib', 'df_type': 'pandas'},
        # {'engine': 'cudf', 'df_type': 'dask'},
        # {'engine': 'cudf', 'df_type': 'pandas'},
    ]
    benchmarks = []
    for engine_config in tqdm.tqdm(engine_configs, desc='Running benchmarks with different configs'):
        benchmarks.append(Benchmark(
            file_dir=f'{output_folder}/ks_yellow_taxi_tripdata_2009-01.parquet',
            **engine_config 
        ))
        benchmarks[-1].run_benchmark()


if __name__ == '__main__':
    main()
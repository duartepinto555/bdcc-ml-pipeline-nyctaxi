import json

import tqdm
import datetime as dt
import download_taxi_datasets
from benchmarks.benchmark_class import Benchmark


def main():    
    # First make sure everything is downloaded correctly
    output_folder = download_taxi_datasets.main(
        years=range(2011,2013),
        partition_size='250MB',
        sample_size=6,
    )

    # Run benchmarks for all engines
    engine_configs = [
        {'engine': 'pandas', 'df_type': 'pandas'},
        # {'engine': 'modin', 'df_type': 'dask', 'dask_init_args': {
        #         'n_workers': 1,
        #         'threads_per_worker': 3,
        #         'memory_limit': '21GiB'
        #     }
        # },
        # {'engine': 'modin', 'df_type': 'ray'},
        # {'engine': 'modin', 'df_type': 'unidist'},
        {
            'engine': 'dask', 'df_type': 'dask', 'dask_init_args': {
                'n_workers': 4,
                'threads_per_worker': 1,
                'memory_limit': '5GiB'
            }
        },
        {'engine': 'joblib', 'df_type': 'pandas'},
        {'engine': 'cudf', 'df_type': 'dask', 'dask_init_args': {
                'n_workers': 4,
                'threads_per_worker': 1,
                'memory_limit': '5GiB'
            }
        },
        {'engine': 'cudf', 'df_type': 'pandas'},
    ]
    benchmarks = []
    for engine_config in tqdm.tqdm(engine_configs, desc='Running benchmarks with different configs'):
        benchmarks.append(Benchmark(
            file_dir=f'{output_folder}',
            **engine_config 
        ))
        benchmarks[-1].run_benchmark()
        
        
    # Save results to json file
    results_file = '/'.join(__file__.split('/')[:-1]) if '/' in __file__ else '/'.join(__file__.split('\\')[:-1])
    results_file = f'{results_file}/../results/benchmark_results_v{dt.datetime.now().strftime("%Y%m%d%H%M%S")}.json'
    with open(results_file, 'w') as f:
        json.dump([b.get_results() for b in benchmarks], f, indent=4)
    


if __name__ == '__main__':
    main()
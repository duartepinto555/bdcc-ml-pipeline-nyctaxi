import os
import time
import logging
import traceback
import numpy as np
import modin.pandas as mpd
import dask.dataframe as dd

from typing import Literal
from joblib import Parallel, delayed
from dask.distributed import Client, LocalCluster

logging.basicConfig(level=logging.INFO, filename="benchmark_class.log", filemode='w', format='%(funcName)s %(lineno)d %(levelname)s %(name)s: %(message)s')


class Benchmark:

    def __init__(
            self,
            file_dir,
            engine: Literal['pandas', 'modin', 'dask', 'cudf', 'joblib']='pandas',
            df_type: Literal['pandas', 'dask', 'ray', 'unidist']='pandas',
            dask_init_args: dict={'n_workers': 1, 'threads_per_worker': 2, 'memory_limit': '20GiB'},
            ray_init_args: dict={'num_cpus': 4, 'num_gpus': 0},
        ):
        self.engine = engine # engine can be 'pandas', 'modin', 'dask', 'cudf', or 'joblib'
        self.df_type = df_type # df_type can be 'pandas', 'dask'
        self.file_dir = file_dir
        self.exec_times = {}
        self.client = None
        if engine == 'modin':
            os.environ["MODIN_ENGINE"] = df_type if df_type != 'pandas' else 'dask'  # Modin will use Dask if nothing else provided
            if os.environ['MODIN_ENGINE'] == 'dask': 
                self.cluster = LocalCluster(**dask_init_args)
                self.client = Client(self.cluster)
                logging.info(f'({self.engine}, {self.df_type}): Starting dask client. Dashboard URL -> {self.client.dashboard_link}')
            elif os.environ['MODIN_ENGINE'] == 'ray':
                import ray
                ray.init(**ray_init_args)
            elif os.environ['MODIN_ENGINE'] == 'unidist': os.environ["UNIDIST_BACKEND"] = "mpi"  # Unidist will use MPI backend
            self.pd = mpd
        elif engine == 'dask':
            self.cluster = LocalCluster(**dask_init_args)
            self.client = Client(self.cluster)
            self.pd = dd
        elif engine == 'cudf': # we can use cudf or dd 
            if df_type == 'dask':
                import dask
                dask.config.set({"dataframe.backend": "cudf"})
                self.cluster = LocalCluster(**dask_init_args)
                self.client = Client(self.cluster)
                self.pd = dd
            else:
                import cudf.pandas # RapidsAI
                cudf.pandas.install()
                import pandas as pd
                self.pd = pd
        elif engine in ('joblib', 'pandas'):
            import pandas as pd
            self.pd = pd
        else: raise ValueError(f'Engine <{engine}> Invalid! Please insert a valid engine')

        # Read initial parquet
        self.df = self.read_file_parquet()
    
        # Create the other columns to do the joins
        self.other = self.groupby_statistics()
        self.other.columns = [e[0] + '_' + e[1] for e in self.other.columns.tolist()]

    def time_decorator(func):
        def wrapper(*args, **kwargs):
            self = args[0]
            start_time = time.time()
            logging.info(f'({self.engine}, {self.df_type}): Starting {func.__name__}')
            try: result = func(*args, **kwargs)
            except Exception as e: 
                logging.info(f'({self.engine}, {self.df_type}): Failed running {func.__name__}. Error: {"".join(traceback.format_tb(e.__traceback__))}')
                end_time = time.time()
                execution_time = end_time - start_time
                args[0].exec_times[func.__name__] = f'Failed after {execution_time} seconds'
                return
            logging.info(f'({self.engine}, {self.df_type}): Finished {func.__name__}')
            end_time = time.time()
            execution_time = end_time - start_time
            args[0].exec_times[func.__name__] = execution_time
            return result
        return wrapper
    
    @time_decorator
    def read_file_parquet(self):
        # Read parquet with index as keyward (if it fails, try without it)
        result = self.pd.read_parquet(self.file_dir)
        return result

    @time_decorator
    def count(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return len(self.df.compute())
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(len)(self.df)]) # We still have to define the chunks size.
        else:
            return len(self.df)

    @time_decorator
    def count_index_length(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return len(self.df.index.compute())
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(len)(self.df.index)]) # We still have to define the chunks size.
        else:
            return len(self.df.index)

    @time_decorator
    def mean(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return self.df.fare_amt.mean().compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.mean)(self.df.fare_amt)]) # We still have to define the chunks size.
        else:
            return self.df.fare_amt.mean()

    @time_decorator
    def standard_deviation(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return self.df.fare_amt.std().compute()
        elif self.engine == 'joblib': 
            return Parallel(n_jobs=-1)([delayed(np.std)(self.df.fare_amt)]) # We still have to define the chunks size.
        else:
            return self.df.fare_amt.std()

    @time_decorator
    def mean_of_sum(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return (self.df.fare_amt + self.df.tip_amt).mean().compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.mean)(self.df.fare_amt + self.df.tip_amt)]) # We still have to define the chunks size.
        else:
            return (self.df.fare_amt + self.df.tip_amt).mean()

    @time_decorator
    def sum_columns(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return (self.df.fare_amt + self.df.tip_amt).compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.sum)(self.df.fare_amt + self.df.tip_amt)]) # We still have to define the chunks size.
        else:
            return (self.df.fare_amt + self.df.tip_amt)

    @time_decorator
    def mean_of_product(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return (self.df.fare_amt * self.df.tip_amt).mean().compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.mean)(self.df.fare_amt * self.df.tip_amt)]) # We still have to define the chunks size.
        else:
            return (self.df.fare_amt * self.df.tip_amt).mean()

    @time_decorator
    def product_columns(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return (self.df.fare_amt * self.df.tip_amt).compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.prod)(self.df.fare_amt * self.df.tip_amt)]) # We still have to define the chunks size.
        else:
            return (self.df.fare_amt * self.df.tip_amt)

    @time_decorator
    def value_counts(self):
        if self.engine == 'dask' or self.df_type == 'dask':
            return self.df.fare_amt.value_counts().compute()
        else:
            return self.df.fare_amt.value_counts()

    @time_decorator
    def mean_of_complicated_arithmetic_operation(self):
        theta_1 = self.df.start_lon
        phi_1 = self.df.start_lat
        theta_2 = self.df.end_lon
        phi_2 = self.df.end_lat
        temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
               + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
        ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
        if self.engine == 'dask' or self.df_type == 'dask':
            return ret.mean().compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.mean)(ret)]) # We still have to define the chunks size.
        else:
            return ret.mean()

    @time_decorator
    def complicated_arithmetic_operation(self):
        theta_1 = self.df.start_lon
        phi_1 = self.df.start_lat
        theta_2 = self.df.end_lon
        phi_2 = self.df.end_lat
        temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
               + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
        ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
        if self.engine == 'dask' or self.df_type == 'dask':
            return ret.compute()
        else:
            return ret

    @time_decorator
    def groupby_statistics(self):
        result = self.df.groupby(by='passenger_count').agg(
          {
            'fare_amt': ['mean', 'std'], 
            'tip_amt': ['mean', 'std']
          }
        )
        if self.engine == 'dask' or self.df_type == 'dask':
            return result.compute()
        else:
            return result
    
    @time_decorator
    def join_count(self):
        return len(self.pd.merge(self.df, self.other, left_index=True, right_index=True))

    @time_decorator
    def join_data(self):
        result = self.pd.merge(self.df, self.other, left_index=True, right_index=True)
        if self.engine == 'dask' or self.df_type == 'dask': return result.compute()
        return result

    def clear_cache(self):
        if self.client:
            self.client.close()
            self.client.shutdown()
        del(self.df)
        del(self.other)

    def run_benchmark(self):
        # self.read_file_parquet()  # Already was executed in the __init__ method
        self.count()
        self.count_index_length()
        self.mean()
        self.standard_deviation()
        self.mean_of_sum()
        # self.sum_columns()            # Not much sense in calculating a pandas dataframe from a df we don't have memory to store
        self.mean_of_product()
        # self.product_columns()        # Not much sense in calculating a pandas dataframe from a df we don't have memory to store
        self.value_counts()
        self.mean_of_complicated_arithmetic_operation()
        # self.complicated_arithmetic_operation()   # Not much sense in calculating a pandas dataframe from a df we don't have memory to store
        # self.groupby_statistics()  # Already was executed in the __init__ method
        self.join_count()
        self.join_data()

        # Finalize everything after running benchmark
        self.clear_cache()

    def get_results(self):
        return ({'engine': self.engine, 'df_type': self.df_type, 'results': self.exec_times})

if __name__ == '__main__':
    input_folder = '/'.join(__file__.split('/')[:-3]) if '/' in __file__ else '/'.join(__file__.split('\\')[:-3]) + '/datasets'
    input_file = f'{input_folder}/ks_yellow_taxi_tripdata_2009-01.parquet'

    # Testing modin
    os.environ["MODIN_ENGINE"] = "dask"
    cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_limit='12GB')
    client = Client(cluster)
    df = mpd.read_parquet(input_file)


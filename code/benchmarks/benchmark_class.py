import os
import time
import logging
import numpy as np
import pandas as pd
import modin.pandas as mpd
import dask.dataframe as dd
# import cudf # RapidsAI
# import dask_cudf

from typing import Literal
from joblib import Parallel, delayed
from dask.distributed import Client, LocalCluster


class Benchmark:

    def __init__(
            self,
            file_dir,
            engine: Literal['pandas', 'modin', 'dask', 'cudf', 'joblib']='pandas',
            df_type: Literal['pandas', 'dask']='pandas',
            cluster=None,
        ):
        self.engine = engine # engine can be 'pandas', 'modin', 'dask', 'cudf', or 'joblib'
        self.df_type = df_type # df_type can be 'pandas', 'dask'
        self.file_dir = file_dir
        self.exec_times = {}
        self.client = None
        # Default "engine"
        self.pd = pd
        if engine == 'modin':
            os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask
            self.cluster = cluster if cluster is not None else LocalCluster(n_workers=1, memory_limit='12GB')
            self.client = Client(self.cluster)
            self.pd = mpd
        elif engine == 'dask':
            self.cluster = cluster if cluster is not None else LocalCluster(n_workers=1, memory_limit='12GB')
            self.client = Client(self.cluster)
            self.pd = dd
        elif engine == 'cudf': # we can use cudf or dd 
            import cudf # RapidsAI
            import dask_cudf
            if df_type == 'dask':
                self.pd = dask_cudf
            else:
                self.pd = cudf
        elif engine == 'joblib':
            self.pd = pd

        # Read parquet with index as keyward (if it fails, try without it)
        try: self.df = self.pd.read_parquet(self.file_dir, index='index')
        except: self.df = self.pd.read_parquet(self.file_dir)
    
        # Create the other columns to do the joins
        self.other = self.groupby_statistics()
        self.other.columns = pd.Index([e[0] + '_' + e[1] for e in self.other.columns.tolist()])

    def time_decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logging.info(f'Starting {func.__name__}')
            result = func(*args, **kwargs)
            logging.info(f'Finished {func.__name__}')
            end_time = time.time()
            execution_time = end_time - start_time
            args[0].exec_times[func.__name__] = execution_time
            return result
        return wrapper
    
    @time_decorator
    def read_file_parquet(self):
        # Read parquet with index as keyward (if it fails, try without it)
        try: result = self.pd.read_parquet(self.file_dir, index='index')
        except: result = self.pd.read_parquet(self.file_dir)
        return result

    @time_decorator
    def count(self):
        if self.engine in ['dask', 'cudf']:
            return len(self.df.compute())
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(len)(self.df)]) # We still have to define the chunks size.
        else:
            return len(self.df)

    @time_decorator
    def count_index_length(self):
        if self.engine in ['dask', 'cudf']:
            return len(self.df.index.compute())
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(len)(self.df.index)]) # We still have to define the chunks size.
        else:
            return len(self.df.index)

    @time_decorator
    def mean(self):
        if self.engine in ['dask', 'cudf']:
            return self.df.fare_amt.mean().compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.mean)(self.df.fare_amt)]) # We still have to define the chunks size.
        else:
            return self.df.fare_amt.mean()

    def standard_deviation(self):
        if self.engine in ['dask', 'cudf']:
            return self.df.fare_amt.std().compute()
        elif self.engine == 'joblib': 
            return Parallel(n_jobs=-1)([delayed(np.std)(self.df.fare_amt)]) # We still have to define the chunks size.
        else:
            return self.df.fare_amt.std()

    @time_decorator
    def mean_of_sum(self):
        if self.engine in ['dask', 'cudf']:
            return (self.df.fare_amt + self.df.tip_amt).mean().compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.mean)(self.df.fare_amt + self.df.tip_amt)]) # We still have to define the chunks size.
        else:
            return (self.df.fare_amt + self.df.tip_amt).mean()

    @time_decorator
    def sum_columns(self):
        if self.engine in ['dask', 'cudf']:
            return (self.df.fare_amt + self.df.tip_amt).compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.sum)(self.df.fare_amt + self.df.tip_amt)]) # We still have to define the chunks size.
        else:
            return (self.df.fare_amt + self.df.tip_amt)

    @time_decorator
    def mean_of_product(self):
        if self.engine in ['dask', 'cudf']:
            return (self.df.fare_amt * self.df.tip_amt).mean().compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.mean)(self.df.fare_amt * self.df.tip_amt)]) # We still have to define the chunks size.
        else:
            return (self.df.fare_amt * self.df.tip_amt).mean()

    @time_decorator
    def product_columns(self):
        if self.engine in ['dask', 'cudf']:
            return (self.df.fare_amt * self.df.tip_amt).compute()
        elif self.engine == 'joblib':
            return Parallel(n_jobs=-1)([delayed(np.prod)(self.df.fare_amt * self.df.tip_amt)]) # We still have to define the chunks size.
        else:
            return (self.df.fare_amt * self.df.tip_amt)

    @time_decorator
    def value_counts(self):
        if self.engine in ['dask', 'cudf']:
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
        if self.engine in ['dask', 'cudf']:
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
        if self.engine in ['dask', 'cudf']:
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
        if self.engine == 'dask':
            return result.compute()
        else:
            return result
    
    @time_decorator
    def join_count(self):
        if self.engine == 'dask':
            return len(self.pd.merge(self.df, self.other, left_index=True, right_index=True))
    
    @time_decorator
    def join_data(self):
        if self.engine == 'dask':
            return self.pd.merge(self.df, self.other, left_index=True, right_index=True).compute()

    def clear_cache(self):
        if self.client: self.client.close()
        del(self.df)
        del(self.other)

    def run_benchmark(self):
        self.read_file_parquet()
        self.count()
        self.count_index_length()
        self.mean()
        self.standard_deviation()
        self.mean_of_sum()
        # self.sum_columns()
        self.mean_of_product()
        # self.product_columns()
        self.value_counts()
        self.mean_of_complicated_arithmetic_operation()
        # self.complicated_arithmetic_operation()
        self.groupby_statistics()
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


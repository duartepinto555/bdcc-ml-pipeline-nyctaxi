import os
import time
import numpy as np
import pandas as pd
import modin.pandas as mpd
import dask.dataframe as dd



class dataframe_benchmark:

    def __init__(self, file_dir, engine='pandas'):
        self.engine = engine # engine can be 'pandas', 'modin' or 'dask'
        self.exec_times = {}
        if engine == 'modin':
            os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask
            self.pd = mpd
            self.df = self.pd.read_parquet(file_dir, index='index')
        elif engine == 'dask':
            self.df = dd.read_parquet(file_dir, index='index')
        else:
            self.pd = pd
            self.df = self.pd.read_parquet(file_dir, index='index')
    
    def time_decorator(self, func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            self.exec_times[func.__name__] = execution_time
        return wrapper
    
    @time_decorator
    def count(self):
        if self.engine == 'dask':
            return len(self.df.compute())
        else:
            return len(self.df)

    time_decorator
    def count_index_length(self):
        if self.engine == 'dask':
            return len(self.df.index.compute())
        else:
            return len(self.df.index)

    time_decorator
    def mean(self):
        if self.engine == 'dask':
            return self.df.fare_amt.mean().compute()
        else:
            return self.df.fare_amt.mean()

    time_decorator
    def standard_deviation(self):
        if self.engine == 'dask':
            return self.df.fare_amt.std().compute()
        else:
            return self.df.fare_amt.std()

    time_decorator
    def mean_of_sum(self):
        if self.engine == 'dask':
            return (self.df.fare_amt + self.df.tip_amt).mean().compute()
        else:
            return (self.df.fare_amt + self.df.tip_amt).mean()

    time_decorator
    def sum_columns(self):
        if self.engine == 'dask':
            return (self.df.fare_amt + self.df.tip_amt).compute()
        else:
            return (self.df.fare_amt + self.df.tip_amt)

    time_decorator
    def mean_of_product(self):
        if self.engine == 'dask':
            return (self.df.fare_amt * self.df.tip_amt).mean().compute()
        else:
            return (self.df.fare_amt * self.df.tip_amt).mean()

    time_decorator
    def product_columns(self):
        if self.engine == 'dask':
            return (self.df.fare_amt * self.df.tip_amt).compute()
        else:
            return (self.df.fare_amt * self.df.tip_amt)

    time_decorator
    def value_counts(self):
        if self.engine == 'dask':
            return self.df.fare_amt.value_counts().compute()
        else:
            return self.df.fare_amt.value_counts()

    time_decorator
    def mean_of_complicated_arithmetic_operation(self):
        theta_1 = self.df.start_lon
        phi_1 = self.df.start_lat
        theta_2 = self.df.end_lon
        phi_2 = self.df.end_lat
        temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
               + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
        ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
        if self.engine == 'dask':
            return ret.mean().compute()
        else:
            return ret.mean()

    time_decorator
    def complicated_arithmetic_operation(self):
        theta_1 = self.df.start_lon
        phi_1 = self.df.start_lat
        theta_2 = self.df.end_lon
        phi_2 = self.df.end_lat
        temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
               + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
        ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
        if self.engine == 'dask':
            return ret.compute()
        else:
            return ret

    time_decorator
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
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 14:10:00 2020

@author: asweet
"""

import pandas as pd
import numpy as np
from sqlalchemy.types import Integer, Numeric, String, DateTime
from sqlalchemy import create_engine
import urllib.parse
from sys import platform
from abc import ABC, abstractmethod
import time

def get_conncection_string(driver, server, database, user_name = 'username', password = 'password', dialect = 'mssql'):
    driver_str = driver.replace(' ', '+')
    # connection_string docs https://docs.sqlalchemy.org/en/13/core/engines.html
    return '%s+pyodbc://%s:%s@%s/%s?driver=%s'%(dialect, username, urllib.parse.quote_plus(password), server, database, driver_str)

def get_engine(driver, server, database, dialect, fast_executemany = True):
    connection_string = get_conncection_string(driver, server, database, dialect = dialect)
    return create_engine(connection_string, fast_executemany = fast_executemany)

def does_output_table_exist(driver, output_server, output_database, output_table, dialect):
    engine = get_engine(driver, output_server, output_database, dialect)
    return engine.dialect.has_table(engine.connect(), output_table)

def create_output_table(sql, driver, output_server, output_database, dialect):
    engine = get_engine(driver, output_server, output_database, dialect)
    with engine.begin() as conn:
        conn.execute(sql)
    
class Process(ABC):
    default_schema = 'dbo'
    
    def __init__(self, output_meta, out_dtypes, verbose, dialect = 'mssql', sql_driver = 'sql_driver', use_backend = True):
        """ Base init """        
        self.dialect = dialect
        self.driver = sql_driver
        self.use_backend = use_backend
        self.verbose = verbose
        
        self.output_server = output_meta['server']
        self.output_database = output_meta['database']
        self.output_schema = output_meta['schema']
        self.output_table = output_meta['table']
        
        self.out_dtypes = out_dtypes
        self.output_table_full = '.'.join([self.output_database, self.output_schema, self.output_table])
        
        if use_backend:
            if self.does_output_table_exist() == False:
                try: 
                    self.create_output_table()
                except Exception as e:
                    print('failed to create output table with exception: {}'.format(e))
                
        self.data = None
        self.has_data = False
        self.exceptions = []
        
    def does_output_table_exist(self):
        engine = get_engine(self.driver, self.output_server, self.output_database, self.dialect)
        return engine.dialect.has_table(engine.connect(), self.output_table)
        
    def create_output_table(self):
        if self.verbose:
            print('creating output table: {}'.format(self.output_table_full))
            
        engine = get_engine(self.driver, self.output_server, self.output_database, self.dialect)
        with engine.begin() as conn:
            conn.execute(self.create_sql.format(self.output_database))
            
    def get_truncate_statement(self):
        sql = 'TRUNCATE TABLE {}.{}.{};'.format(self.output_database, self.output_schema, self.output_table)
        return sql
        
    def push_to_sql(self):
        out_cols = list(self.out_dtypes.keys())
        engine = get_engine(self.driver, self.output_server, self.output_database, self.dialect)
        with engine.begin() as conn:
            # truncate table
            conn.execute(self.get_truncate_statement())
            # push updated data
            self.data[out_cols].to_sql(self.output_table, con = conn, index = False, if_exists = 'append', dtype = self.out_dtypes)
            
    @abstractmethod
    def get_data(self):
        """ get data"""
        
    def process(self, push_to_sql = True):
        if self.use_backend == False:
            push_to_sql = False

        start_time = time.time()
        if self.verbose:
            print('getting data')
            
        self.get_data()
        
        if self.verbose:
            print('finished getting data after {} seconds'.format(time.time() - start_time))
        
        if self.has_data:
            if push_to_sql:
                start_time = time.time()
                if self.verbose:
                    print('pushing to sql')
                    
                try:
                    self.push_to_sql()
                except Exception as e:
                    print('failed to push to sql with exception: {}'.format(e))
                    
                if self.verbose:
                    print('finished pushing to sql after {} seconds'.format(time.time() - start_time))
            else:
                return self.data
        else:
            print('no data found')
        
class COVID_19_JHU(Process):
    create_sql = (
        """
            -- create query
        """        
    )
            
    out_dtypes = {
        'FIPS': Integer(), 
        'Admin2': String(50), 
        'Province_State': String(50), 
        'Country_Region': String(50), 
        'Combined_Key': String(128), 
        'Lat': Numeric(18, 7), 
        'Long': Numeric(18, 7), 
        'Confirmed': Integer(), 
        'Deaths': Integer(), 
        'Recovered': Integer(), 
        'Active': Integer(), 
        'Date': DateTime(), 
        'Last_Update': DateTime(), 
    }

    output_meta = {
        'server': 'server',
        'database': 'database',
        'schema': 'schema',
        'table': 'covid_19_jhu',
    }
    
    def __init__(self, verbose = False, use_backend = True):        
        super().__init__(self.output_meta, self.out_dtypes, verbose, use_backend = use_backend)
        
    def get_data(self):
        try:
            # source: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports
            base_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
            
            start_date = '2020-01-22' # first date of data from John Hopkins
            end_date = pd.Timestamp.today()
            date_idx = pd.date_range(start = start_date, end = end_date, freq = 'D')
            
            df_list = []
            for date in date_idx:
                try: 
                    csv_file = date.strftime('%m-%d-%Y') + '.csv'
                    this_csv = base_url + csv_file
                    df = pd.read_csv(this_csv)
                    df['Date'] = date
                    df_list.append(df)
                except Exception as e:
                    print('{} not found'.format(csv_file))
                    self.exceptions.append(e)
                    
            col_names = []
            for df in df_list:
                col_names = col_names + list(df)
                
            col_names_mapping = {
                'Province/State': 'Province_State', 
                'Country/Region': 'Country_Region', 
                'Last Update': 'Last_Update', 
                'Latitude': 'Lat',
                'Longitude': 'Long',
                'Long_': 'Long'
            }
            updated_df_list = []
            for df in df_list:
                for old_name, new_name in col_names_mapping.items():
                    if old_name in df.columns:
                        df = df.rename({old_name: new_name}, axis = 1)
                        if new_name == 'Last_Update':
                            df[new_name] = pd.to_datetime(df[new_name])
                updated_df_list.append(df)
                
            df_append = pd.DataFrame()
            for df in updated_df_list:
                df_append = df_append.append(df, sort = False).reset_index(drop = True)
                
            # some cleaning steps
            df_append['Last_Update'] = pd.to_datetime(df_append['Last_Update'])
            
            country_region_mappings = {
                'Republic of Korea': 'Korea, South',
                'Iran (Islamic Republic of)': 'Iran',
                'Mainland China': 'China'
            }
            for old_val, new_val in country_region_mappings.items():
                if old_val in df_append['Country_Region'].unique():
                    df_append.loc[df_append['Country_Region'] == old_val, 'Country_Region'] = new_val
            
            int_cols = ['Confirmed', 'Deaths', 'Recovered', 'Active', 'FIPS']
            for col in int_cols:
                if col in df_append.columns:
                    df_append[col] = df_append[col].astype('Int64') # nullable integer type
                    
            self.data = df_append
            self.has_data = True
        except Exception as e:
            print('failed with exception {}'.format(e))
    
class COVID_19_SFC(Process):
    create_sql = (
        """
            -- create query
        """        
    )
            
    out_dtypes = {
        'geography': String(50), 
        'bay_area': String(4), 
        'cases': Integer(), 
        'deaths': Integer(), 
        'running_total_of_cases': Integer(), 
        'running_total_of_deaths': Integer(), 
        'date': DateTime(), 
    }

    output_meta = {
        'server': 'server',
        'database': 'database',
        'schema': 'schema',
        'table': 'covid_19_sfc',
    }
    
    def __init__(self, verbose = False, use_backend = True):        
        super().__init__(self.output_meta, self.out_dtypes, verbose, use_backend = use_backend)
            
    @staticmethod
    def fetch_sfc_json():
        json_url = 'https://sfc-project-files.s3.amazonaws.com/project-feeds/covid19_us_cases_ca_by_county_.json'
        return pd.read_json(json_url)
        
    def get_data(self):
        try:
            df_in = self.fetch_sfc_json()
            
            df_in = df_in.drop(['TOTALS', 'ROW'], axis = 1)
            df_unpivot = df_in.melt(id_vars = ['GEOGRAPHY', 'BAY AREA', 'CATEGORY'])
            df_unpivot = df_unpivot.rename({'variable': 'Date'}, axis = 1)
            df_pivot_table = pd.pivot_table(df_unpivot.fillna('_'), values = 'value', index = ['GEOGRAPHY', 'BAY AREA', 'Date'], 
                                              columns = ['CATEGORY'], aggfunc = np.max).reset_index()
            df_pivot_table['deaths'] = df_pivot_table['deaths'].astype(str).replace('', '0').astype(int)
            df_pivot_table['cases'] = df_pivot_table['cases'].astype(str).replace('', '0').astype(int)
            df_pivot_table['BAY AREA'] = df_pivot_table['BAY AREA'].replace('', np.nan)
            df_cumsum = df_pivot_table.groupby(by = ['GEOGRAPHY', 'Date']).sum().groupby(level = [0]).cumsum().reset_index()
            df_cumsum = df_cumsum.rename({'cases': 'running_total_of_cases', 'deaths': 'running_total_of_deaths'}, axis = 1)
            df_output = df_pivot_table.merge(df_cumsum, on = ['GEOGRAPHY', 'Date'])
            df_output['Date'] = pd.to_datetime(df_output['Date'])
            df_output.columns = [col.lower().replace(' ', '_') for col in df_output.columns]
                    
            self.data = df_output
            self.has_data = True
        except Exception as e:
            print('failed with exception {}'.format(e))
            
            
class COVID_19_NYT(Process):
    create_sql = (
        """
            -- create query
        """        
    )
            
    out_dtypes = {
        'fips': String(5), 
        'county': String(50), 
        'state': String(50), 
        'cases': Integer(), 
        'deaths': Integer(), 
        'date': DateTime(),  
    }

    output_meta = {
        'server': 'server',
        'database': 'database',
        'schema': 'schema',
        'table': 'covid_19_nyt',
    }
    
    def __init__(self, verbose = False, use_backend = True):        
        super().__init__(self.output_meta, self.out_dtypes, verbose, use_backend)
        
    def get_data(self):
        try:
            raw_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
            df_nyt = pd.read_csv(raw_url, dtype = {'fips': 'str'})
            df_nyt['date'] = pd.to_datetime(df_nyt['date'])
            df_nyt['cases'] = df_nyt['cases'].astype('Int64')
            df_nyt['deaths'] = df_nyt['deaths'].astype('Int64')
                    
            self.data = df_nyt
            self.has_data = True
        except Exception as e:
            print('failed with exception {}'.format(e))
            
class COVID_19_JHU_US(Process):
    create_sql = (
        """
            -- create sql
        """        
    )
            
    out_dtypes = {
        'uid': String(8),
        'iso2': String(2),
        'iso3': String(3),
        'code3': String(3),
        'fips': String(5),
        'admin2': String(64),
        'province_state': String(50),
        'country_region': String(4),
        'lat': Numeric(18, 7),
        'long': Numeric(18, 7),
        'combined_key': String(64),
        'date': DateTime(), 
        'confirmed': Integer(), 
        'deaths': Integer(),   
    }

    output_meta = {
        'server': 'server',
        'database': 'database',
        'schema': 'schema',
        'table': 'covid_19_jhu_us',
    }
    
    def __init__(self, verbose = False, use_backend = True):        
        super().__init__(self.output_meta, self.out_dtypes, verbose, use_backend)
        
    def get_data(self):
        try:
            url_confirmed = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
            df_jhu_confirmed = pd.read_csv(url_confirmed)
            
            # df_jhu_confirmed['FIPS'] = df_jhu_confirmed['FIPS'].astype('Int64')
            df_confirmed = df_jhu_confirmed.melt(id_vars = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Province_State', 
                                                              'Country_Region', 'Lat', 'Long_', 'Combined_Key']).rename(
                                                    {'variable': 'date', 'value': 'confirmed'}, axis = 1)
            
            url_deaths = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'
            df_jhu_deaths = pd.read_csv(url_deaths)
            
            # df_jhu_deaths['FIPS'] = df_jhu_deaths['FIPS'].astype('Int64')
            df_deaths = df_jhu_deaths.melt(id_vars = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Province_State', 
                                                              'Country_Region', 'Lat', 'Long_', 'Combined_Key']).rename(
                                                    {'variable': 'date', 'value': 'deaths'}, axis = 1)
            
            death_cols = ['UID', 'date', 'deaths']
            df_final = df_confirmed.merge(df_deaths[death_cols], on = ['UID', 'date'])
            df_final.columns = [c.lower() for c in df_final.columns]
            
            df_final['confirmed'] = df_final['confirmed'].astype(int)
            df_final['deaths'] = df_final['deaths'].astype(int)
            df_final['date'] = pd.to_datetime(df_final['date'])
            df_final = df_final.rename({'long_': 'long'}, axis = 1)
            
            str_cols = ['uid', 'iso2', 'iso3', 'code3', 'fips']
            for col in str_cols:
                df_final[col] = df_final[col].fillna('').astype(str)
                    
            self.data = df_final
            self.has_data = True
        except Exception as e:
            print('failed with exception {}'.format(e))
            
def get_processes():
    process_dict = {
        'covid_19_jhu': COVID_19_JHU,
        'covid_19_sfc':  COVID_19_SFC,    
        'covid_19_nyt': COVID_19_NYT,
        'covid_19_jhu_us': COVID_19_JHU_US,
    }
    return process_dict
            
if __name__ == '__main__':        
    for process_name, process in get_processes().items():
        try:
            print('processing: {}'.format(process_name))
            this_process = process(verbose = True)
            this_process.process()
        except Exception as e:
            print('failed to process {} with exception {}'.format(process_name, e))

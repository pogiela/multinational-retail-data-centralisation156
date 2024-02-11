'''
DataExtractor class is a child class of DataProcessing class and contains methods allowing data extraction from various sources like API, S3 bucket, remote URL file, PDF, CSV or JSON file.

Methods:
-------
extract_from_remote_location(remote_data, data_type)
    Extract data from a remote data location (file or API) based on the data_type.
    
    Parameters:
    ----------
    remote_data: string | string[]
        Is a name of the environmental variable or a list of environmental variables when data_type == 'api'. 
        The variable contains an URL to the remote file location which need to be extracted or the API parameters required to extract data from the API.
    data_type: string
        This parameter helps to decide which data extraction method to use and which Pandas method to use to load the data into Pandas DataFrames.
        Allowed options are: api, csv, json and pdf.
        
list_db_tables(engine)
    Returns a list of all tables available in the database defined in the engine parameter.
    
    Parameters:
    ----------
    engine: db_engine
        DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
        
read_rds_table(engine, table_name)
    Returns all data from a table specified in the table_name parameter from the database specified in the engine parameter.
    
    Parameters:
    ----------
    engine: db_engine
        DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
    table_name: string
        Table name fro which the data should be returned.
'''

from data_processing import DataProcessing
from dotenv import load_dotenv
import fitz
import os
import pandas as pd
from sqlalchemy import inspect
import sys


class DataExtractor(DataProcessing):
    def __init__(self):
        # initiate dotenv to load environmental variables from .env file
        load_dotenv()


    def extract_from_remote_location(self, remote_data, data_type):
        '''
        extract_from_remote_location(remote_data, data_type)
            Extract data from a remote data location (file or API) based on the data_type.
            
            Parameters:
            ----------
            remote_data: string | string[]
                Is a name of the environmental variable or a list of environmental variables when data_type == 'api'. 
                The variable contains an URL to the remote file location which need to be extracted or the API parameters required to extract data from the API.
            data_type: string
                This parameter helps to decide which data extraction method to use and which Pandas method to use to load the data into Pandas DataFrames.
                Allowed options are: api, csv, json and pdf.
        '''
        # check if data type is correct
        if data_type == 'api':
            env_variables = {}
            for variable in remote_data:
                env_variables[variable] = os.getenv(variable)
            
            headers = self.__create_headers(env_variables['x_api_key'])
            number_of_stores_api = env_variables['number_of_stores_api']
            retrive_store_api = env_variables['retrive_store_api']
            
            print('\n############## Accessing the API: ##############')
            number_of_stores = super().retrive_data_from_api(number_of_stores_api, headers)['number_stores']
            print(f"\n--> There are {number_of_stores} stores available in the API\n\n")
    
        elif data_type in ['pdf', 'csv', 'json']:
            print('\n############## Accessing the file: ##############')
            remote_data_path = os.getenv(remote_data)
            print(f'\n--> Data link: {remote_data_path}\n\n')
            
            if remote_data_path:
                print('\n############## Checking the file: ##############') 
                # download the file to a temporary folder
                downloaded_file = super().download_file(remote_data_path, data_type)
                print(f"\n-->File accessed successfully.\n\n")
            else:
                # stop the programme if the remote_data_path could not be read from .env file
                print(f'\n--> Error, could not get remote_data_path from .env file.\n\n')
                sys.exit()
        else:
            # stop the programme if the data_type is incorrect
            print(f'\n--> Error, data_type should be one of the following options: pdf, csv, json or api.\n\n')
            sys.exit()
            
        print('\n############## Processing the data: ##############\n\n')
        if data_type == 'json':
            extracted_df = pd.read_json(downloaded_file)
        elif data_type == 'csv':
            extracted_df = pd.read_csv(downloaded_file, index_col=0)
        elif data_type == 'pdf':
            extracted_df = self.__process_pdf_file(downloaded_file, data_type)
        elif data_type == 'api':
            extracted_df = self.__process_api_data(retrive_store_api, number_of_stores, data_type, headers)

        print(f'\n--> Data loaded successfully\n\n')
        print('\n############## First 5 rows of data: ##############\n') 
        print(extracted_df.head())
        print('\n\n############## Data information: ##############\n')
        print(extracted_df.info())
        # return extracted data frame
        return extracted_df
            
            
    def __process_pdf_file(self, file_path, data_type):
        # check the number of pages in the pdf file
        page_count = self.__get_page_count(file_path)
        print(f"--> There are {page_count} pages in the PDF\n\n")
        dfs = super().process_with_progress(file_path, page_count, data_type)
        # concatenate all pages into a single data frame
        try:
            final_df = pd.concat(dfs, ignore_index=True)
        except Exception as e:
            print(f'Error occured when concatenating pages data: {e}')
            sys.exit()

        return final_df
        
        
    def __get_page_count(self, pdf_path):
        try:
            with fitz.open(pdf_path) as doc:
                page_count = doc.page_count
                return page_count
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
            
            
    def __create_headers(self, x_api_key):
        return { 
            "Content-Type": "application/json",
            "x-api-key": x_api_key
        }
   
   
    def __process_api_data(self, retrive_store_api, number_of_stores, data_type, headers):
        dfs = super().process_with_progress(retrive_store_api, number_of_stores, data_type, headers)
        # concatenate all pages into a single data frame
        try:
             # create pandas dataframe and return it
            df = pd.DataFrame(dfs)
            df = df.set_index('index')  # Set 'index' column as the DataFrame index
        except Exception as e:
            print(f'Error occured when retriving data: {e}')
            sys.exit()

        return df
    
    
    def list_db_tables(self, engine):
        '''
        list_db_tables(engine)
            Returns a list of all tables available in the database defined in the engine parameter.
            
            Parameters:
            ----------
            engine: db_engine
                DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
        '''
        # get a list of tables in the db
        if engine:
            try:
                inspector = inspect(engine)
                tables = inspector.get_table_names()
            except Exception as e:
                print(f'Error occured when reading tables from the DB: {e}')
                engine.close()
                sys.exit()
                
            if len(tables) == 0:
                print('There are no tables in the database.')
                return
            elif len(tables) == 1:
                print(f'\n--> There is {len(tables)} table available in the database\n') 
            else:
                print(f'\n--> There are {len(tables)} tables available in the database\n') 
            if len(tables) > 0:
                print('\n############## Available tables: ##############\n') 
                print(tables)
        
            return tables
            
        else:
            print(f'Error, the DB engine was not initiated correctly.')
            sys.exit()
            
            
    def read_rds_table(self, engine, table_name):
        '''
        read_rds_table(engine, table_name)
            Returns all data from a table specified in the table_name parameter from the database specified in the engine parameter.
            
            Parameters:
            ----------
            engine: db_engine
                DB Engine object initiated with the init_db_engine() method from DatabaseConnector class.
            table_name: string
                Table name fro which the data should be returned.
        '''
        # read data from the selected table and creat a panda dataframe
        try:
            data = pd.read_sql_table(table_name, engine)
            
            if 'index' in data:
                data = data.set_index('index')  # Set 'index' column as the DataFrame index
            if 'level_0' in data:
                data.rename(columns={'level_0': 'index'}, inplace=True) # rename column for orders_table
                data = data.set_index('index')
        except Exception as e:
            print(f'Error occured when reading the data from {table_name} table: {e}')
            engine.close()
            sys.exit()
    
        print(f'\n--> {data.shape[0]} rows and {data.shape[1]} columns read from table name {table_name}.\n') 
        print('\n############## First 5 rows of data: ##############\n') 
        print(data.head())
        print('\n\n############## Data information: ##############\n')
        print(data.info())
        return data
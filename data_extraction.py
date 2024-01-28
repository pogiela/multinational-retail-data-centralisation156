# This class will work as a utility class, in it you will be creating methods that help extract
# data from different data sources.
# The methods contained will be fit to extract data from a particular data source, 
# these sources will include CSV files, an API and an S3 bucket.

from sqlalchemy import inspect
import pandas as pd
import yaml

class DataExtractor:
    def __init__(self, db_engine):
        # initiate database engine
        self.db_engine = db_engine
    
    def list_db_tables(self):
        # connect to the db engine
        engine = self.db_engine.connect()
        # get a list of tables in the db
        if engine:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
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
            return inspector.get_table_names()
        else:
            return []
    
    def read_rds_table(self, table_name):
        # read data from the selected table and creat a panda dataframe
        data = pd.read_sql_table(table_name, self.db_engine)
        # print(data.head())
        print(f'\n--> {data.shape[0]} rows and {data.shape[1]} columns read from table name {table_name}.\n') 
        print('\n############## First 5 rows of data: ##############\n') 
        print(data.head())
        print('\n\n############## Data information: ##############\n')
        print(data.info())
        return data
    
    def retrieve_pdf_data(self, link):
        data_link = self.__read_data_link(link)
        print(f'Data link: {data_link}')
    
    def __read_data_link(self, link_name):
        # read the db credentials or throw an error
        try:
            with open('.s3_data.yaml', 'r') as file:
                links = yaml.safe_load(file)
                return links[link_name]
        except FileNotFoundError:
            print('Error: .s3_data.yaml not found.')
            return None
        except yaml.YAMLError as e:
            print(f'Error loading YAML: {e}')
            return None
        
        
    
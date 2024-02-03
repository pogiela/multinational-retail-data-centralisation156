# This class will work as a utility class, in it you will be creating methods that help extract
# data from different data sources.
# The methods contained will be fit to extract data from a particular data source, 
# these sources will include CSV files, an API and an S3 bucket.

from sqlalchemy import inspect
import pandas as pd
import sys

class DataExtractor:
    def __init__(self):
        pass
    
    def list_db_tables(self, engine):
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
        # read data from the selected table and creat a panda dataframe
        try:
            data = pd.read_sql_table(table_name, engine)
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
    

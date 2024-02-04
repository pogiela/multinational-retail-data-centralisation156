# methods to clean data from each of the data sources.
# from database_utils import DatabaseConnector
# from data_extraction import DataExtractor
import pandas as pd
from dateutil.parser import parse
import sys

class DataCleaning:
    def __init__(self):
        self.incorrect_dates = set()
        self.conversion_errors = 0
    
    def clean_user_data(self, input_data, string_columns=[], date_columns=[], number_columns=[], int_columns=[]):
        print('\n############## Changing column types: ##############\n') 
        try:
            cleaned_data = self.__change_column_types(input_data, string_columns, date_columns, number_columns)
        except Exception as e:
            print(f'Error occured when trying to change the column types: {e}')
            sys.exit()
        
        print('----> Data type changed successfully\n')
        
        # display datetime conversion errors
        if self.conversion_errors > 0:
            print(f'\n############## Following {len(self.incorrect_dates)} items could not be converted to a datetime ({self.conversion_errors} errors in total): ##############\n')
            print(self.incorrect_dates)
            
        print('\n\n############## Data information after column types changed: ##############\n')
        print(cleaned_data.info())
        
        print('\n\n############## Filtering blank columns and rows: ##############\n') 
        # get the number of minimum non-blank columns to keep in the df 
        # all columns minus dates and numbers = if all dates and numbers are blank 
        # then these rows are removed (at least one of these need to be non-blank)
        blank_columns_thresh = cleaned_data.shape[1] - len(date_columns) - len(number_columns) + 1
        try:
            filtered_data = self.__filter_out_blanks(cleaned_data, blank_columns_thresh)
        except Exception as e:
            print(f'Error occured when trying to filter the data: {e}')
            sys.exit()
        
        print(f'----> {input_data.shape[1] - filtered_data.shape[1]} blanks columns removed.\n')
        print(f'----> {input_data.shape[0] - filtered_data.shape[0]} blanks rows removed.\n')

        # Replace any null values in numeric columns with 0
        ## get list of datatypes
        data_types = filtered_data.dtypes 
        ## get list of numeric columns (in case any of the original columns were dropped)
        selected_columns = data_types[data_types.isin(['float64', 'int64'])]
        numeric_columns = selected_columns.index.tolist()
        if len(numeric_columns) > 0:
            try:
                for column in numeric_columns:
                    filtered_data[column]=filtered_data[column].fillna(0) # replace blanks with 0
            except Exception as e:
                print(f"An error occurred: {e}")
                
        # Update Int columns type
        ## get list of numeric columns (in case any of the original columns were dropped)
        selected_columns = data_types[data_types.isin(['int64'])]
        integer_columns = selected_columns.index.tolist()
        if len(integer_columns) > 0:
            try:
                for column in integer_columns:
                    filtered_data[column]=filtered_data[column].astype('int64')
            except Exception as e:
                print(f"An error occurred: {e}")
                
        print('\n############## Data information after blank rows removed: ##############\n')
        print(filtered_data.info())
        
        
        
        return filtered_data
        
    def __change_column_types(self, input_data, string_columns=[], date_columns=[], number_columns=[]):
        df = input_data
        
        # Change string column types to string
        try:
            df[string_columns] = df[string_columns].astype('string')
        except Exception as e:
            print(f"An error occurred: {e}")
            
        # Parse date columns to datetime
        try:
            for column in date_columns:
                df[column] = df[column].apply(self.__parse_date)
                df[column] = pd.to_datetime(df[column], errors='coerce')
        except Exception as e:
            print(f"An error occurred: {e}")
        
        # Change numeric column types
        if len(number_columns) > 0:
            try:
                for column in number_columns:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
            except Exception as e:
                print(f"An error occurred: {e}")
                
        return df

    def __parse_date(self, date_str):
        # helper method to parse the strings to dates
        try:
            return parse(date_str)
        except Exception:
            # Add problematic values to the incorrect_dates set
            self.incorrect_dates.add(date_str)
            # Increase the conversion_errors count
            self.conversion_errors += 1
            return None
    
    def __filter_out_blanks(self, df, blank_columns_thresh):
        # drop columns where all data is blank
        drop_col_df = df.dropna(axis=1, how='all')
        # drop rows where all numeric and date columns are blank
        drop_df = drop_col_df.dropna(axis=0, thresh=blank_columns_thresh)
        return drop_df
        
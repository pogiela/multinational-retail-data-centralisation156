# methods to clean data from each of the data sources.
# from database_utils import DatabaseConnector
# from data_extraction import DataExtractor
import pandas as pd
from dateutil.parser import parse


class DataCleaning:
    def __init__(self, input_data):
        self.input_data = input_data
        self.incorrect_dates = set()
        self.conversion_errors = 0
    
    def clean_user_data(self):
        print('\n############## Changing column types: ##############\n') 
        cleaned_data = self.__change_column_types(self.input_data)
        print('----> Data type changed successfully\n')
        if self.conversion_errors > 0:
            print(f'\n############## Following {len(self.incorrect_dates)} items could not be converted to a datetime ({self.conversion_errors} errors in total): ##############\n')
        print(self.incorrect_dates)
        print('\n\n############## Data information after column types changed: ##############\n')
        print(cleaned_data.info())
        print('\n\n############## Filtering blank rows: ##############\n') 
        filtered_data = self.__filter_out_blank_rows(cleaned_data)
        print(f'----> {self.input_data.shape[0] - filtered_data.shape[0]} blanks rows removed.\n')
        print('\n############## Data information after blank rows removed: ##############\n')
        print(filtered_data.info())
        return filtered_data
        
    def __change_column_types(self, df):
        string_columns = ['first_name', 'last_name', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'user_uuid']
        date_columns = ['date_of_birth', 'join_date']
        
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
    
    def __filter_out_blank_rows(self, df):
        # drop all rows where any of the columns is blank
        drop_df = df.dropna(axis=0)
        return drop_df
        
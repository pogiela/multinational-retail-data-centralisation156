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
        
    
    def __convert_product_weights(self, df):
        df['weight'] = df['weight'].apply(self.__extract_weight).astype(float)
        return df
        
        
    def __extract_weight(self, weight_str):
        try:
            # Remove leading/trailing whitespace, special characters and convert to lowercase
            weight_str = weight_str.strip().lower()
            # Remove space
            weight_str = weight_str.replace(' ', '')
            
            # Check if the weight is in a format 5x300g and then get the final weight
            if weight_str.find('x') != -1:
                weight_str = weight_str.replace('g', '')
                values = [int(s) for s in weight_str.split('x')]
                return (values[0] * values[1]) / 1000
            # check if weight is in kg
            elif weight_str.find('kg') != -1:
                return float(weight_str.replace('kg', ''))
            # check if weight is in oz
            elif weight_str.find('oz') != -1:
                return float(weight_str.replace('oz', '')) * 0.0283495
            # check if weight is in g
            elif weight_str.find('g') != -1:
                return float(weight_str.replace('g', '')) / 1000
            # check if weight is in ml
            elif weight_str.find('ml') != -1:
                return float(weight_str.replace('ml', '')) / 1000
            else:
                raise ValueError(f"Unsupported unit: {weight_str}")
        except Exception as e:
            print("Error processing weight:", weight_str, e)
            return None 
        
        
    def clean_products_data(self, df):
        # convert product weight into kg
        print('\n\n############## Converting weight into decimal numbers in kg: ##############\n') 
        try:
            df = self.__convert_product_weights(df)
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        print('----> Product weight column changed successfully\n')
        
        # converts product price into a number
        print('\n\n############## Converting price into decimal numbers: ##############\n') 
        try:
            df['product_price'] = df['product_price'].replace('£', '', regex=True).astype(float)
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        print('----> Product price column changed successfully\n')
        
        print('\n############## Data information after weight and price converted: ##############\n')
        print(df.info())
        
        # return final dataframe
        return df
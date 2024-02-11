'''
DataCleaning class contains methods helping to clean the data, change data types, 
remove rows and columns to prepare data for upload to the output database

Methods:
-------
clean_user_data(input_data, string_columns=[], date_columns=[], number_columns=[], integer_columns=[])
    Changes column types based on the list of columns passed as the parameters for the data frame passed as input_data parameter. This method will also remove any blank columns or rows.
    
    Parameters:
    ----------
    input_data: DataFrame
        A source DataFrame in which the data will be cleaned.
    string_columns: string[],
    date_columns: string[], 
    number_columns: string[], 
    integer_columns: string[]
        List of columns from the source DataFrame which will be converted to a data type based on the parameter name (e.g. string, dates, numbers and integers)
        
clean_products_data(df)
    Cleans Products data. This extracts and converts weight column to kg and price column.
    
    Parameters:
    ----------
    df: DataFrame
        A source DataFrame in which the data will be cleaned.
        
clean_orders_data(df, columns_to_remove)
    Cleans orders table. More specifically, removes unwanted columns.
    
    Parameters:
    ----------
    df: DataFrame
        A source DataFrame in which the data will be cleaned.
    columns_to_remove: string[]
        A list of columns to be removed
'''

from numpy import integer
import pandas as pd
from dateutil.parser import parse
import sys


class DataCleaning:
    def __init__(self):
        self.incorrect_dates = set()
        self.conversion_errors = 0
    
    
    def clean_user_data(self, input_data, string_columns=[], date_columns=[], number_columns=[], integer_columns=[]):
        '''
        clean_user_data(input_data, string_columns=[], date_columns=[], number_columns=[], integer_columns=[])
            Changes column types based on the list of columns passed as the parameters for the data frame passed as input_data parameter. 
            This method will also remove any blank columns or rows.
        
            Parameters:
            ----------
            input_data: DataFrame
                A source DataFrame in which the data will be cleaned.
            string_columns: string[],
            date_columns: string[], 
            number_columns: string[], 
            integer_columns: string[]
                List of columns from the source DataFrame which will be converted to a data type based on the parameter name (e.g. string, dates, numbers and integers)
        '''
        print('\n############## Changing column types: ##############\n') 
        try:
            cleaned_data = self.__change_column_types(input_data, string_columns, date_columns, number_columns)
        except Exception as e:
            print(f'Error occured when trying to change the column types: {e}')
            sys.exit()
        
        print('\n\n----> Success. Data type changed successfully\n')
        
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
        blank_columns_thresh = 1
        if len(date_columns) > 0 or len(number_columns) > 0: # this is in case there are no numeric or date columns
            blank_columns_thresh = cleaned_data.shape[1] - len(date_columns) - len(number_columns) + 1
        try:
            filtered_data = self.__filter_out_blanks(cleaned_data, blank_columns_thresh)
        except Exception as e:
            print(f'Error occured when trying to filter the data: {e}')
            sys.exit()
        
        print(f'----> {input_data.shape[1] - filtered_data.shape[1]} blanks columns removed.\n')
        print(f'----> {input_data.shape[0] - filtered_data.shape[0]} blanks rows removed.\n')
                
        # Update Int columns type
        ## get list of numeric columns (in case any of the original columns were dropped)
        float_cols = [col for col in filtered_data.columns if filtered_data[col].dtype == 'float64']
        ## final list of columns to convert to Int (which are in the df and in the original integer_columns list)
        int_columns = list(set(float_cols) & set(integer_columns))
        if len(int_columns) > 0:
            try:
                for column in int_columns:
                    filtered_data[column]=filtered_data[column].fillna(0).astype('int64', errors='raise')
            except Exception as e:
                print(f"An error occurred: {e}")
        
        # update time columns to show just time (no date):
        if 'timestamp' in filtered_data:
            filtered_data['timestamp'] = pd.to_datetime(filtered_data['timestamp']).dt.time
        
        print('\n############## Data information after blank rows removed: ##############\n')
        print(filtered_data.info())
        print('\n############## First 5 rows of data: ##############\n')
        print(filtered_data.head())
        
        return filtered_data
      
        
    def __change_column_types(self, input_data, string_columns=[], date_columns=[], number_columns=[]):
        df = input_data
        
        # Change string column types to string
        if len(string_columns) > 0:
            print(f"\n----> String columns: {string_columns}\n")
            try:
                df[string_columns] = df[string_columns].astype('string')
                print(f"    ---> Columns {string_columns} changed to string\n")
            except Exception as e:
                print(f"An error occurred: {e}")
            
        # Parse date columns to datetime
        if len(date_columns) > 0:
            print(f"\n----> Datetime columns: {date_columns}\n")
            try:
                for column in date_columns:
                    print(f"    ---> Chaning column {column} to datetime")
                    df[column] = df[column].apply(self.__parse_date)
                    df[column] = pd.to_datetime(df[column], errors='coerce')
            except Exception as e:
                print(f"An error occurred: {e}")
        
        # Change numeric column types
        if len(number_columns) > 0:
            print(f"\n----> Numeric columns: {number_columns}\n")
            try:
                for column in number_columns:
                    print(f"    ---> Chaning column {column} to float")
                    df[column] = pd.to_numeric(df[column], errors='coerce')
            except Exception as e:
                print(f"An error occurred: {e}")
        
        # Change integer column types
        # if len(integer_columns) > 0:
        #     print(f"\n----> Integer columns: {integer_columns}\n")
        #     try:
        #         for column in integer_columns:
        #             print(f"    ---> Chaning column {column} to int")
        #             df[column] = df[column].fillna(0).astype('int64', errors='raise')
        #     except Exception as e:
        #         print(f"An error occurred: {e}")
                
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
        '''
        clean_products_data(df)
            Cleans Products data. This extracts and converts weight column to kg and price column.
        
            Parameters:
            ----------
            df: DataFrame
                A source DataFrame in which the data will be cleaned.
        '''
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
    
    
    def clean_orders_data(self, df, columns_to_remove):
        '''
        clean_orders_data(df, columns_to_remove)
            Cleans orders table. More specifically, removes unwanted columns.
            
            Parameters:
            ----------
            df: DataFrame
                A source DataFrame in which the data will be cleaned.
            columns_to_remove: string[]
                A list of columns to be removed
        '''
        
        print('\n\n############## Removing specified coloumns ##############\n')
        try:
            df.drop(columns=columns_to_remove, inplace=True)
            print(f'----> Success, columns {columns_to_remove} removed.\n')
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
       
        print('\n############## Data information after columns removed: ##############\n')
        print(df.info())
        
        return df
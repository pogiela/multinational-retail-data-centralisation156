import boto3
import sys
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd


class CsvDataExtractor:
    def __init__(self):
        # initiate local variables
        load_dotenv()
        self.products_data_address = os.getenv('PRODUCTS_DATA')
        self.temporary_folder = './temp_files'
        self.temporary_file = 'products.csv'


    def extract_from_s3(self):
        print('\n############## Accessing the S3 bucket: ##############')
        print(f'\n--> Data link: {self.products_data_address}\n\n')
        
        if self.products_data_address:
            print('\n############## Checking the file: ##############') 
            # download the pdf file to a temporary folder
            downloaded_csv_file = self.__download_csv_file()
            
            print(f"\n-->File accessed successfully.\n\n")
            print('\n############## Processing the file: ##############\n\n') 
            final_df = pd.read_csv(downloaded_csv_file, index_col=0)
            print(f'\n--> Data loaded successfully\n\n')
            print('\n############## First 5 rows of data: ##############\n') 
            print(final_df.head())
            print('\n\n############## Data information: ##############\n')
            print(final_df.info())
            
            return final_df
            
            
    def __download_csv_file(self):
        # Create a temporary folder if not exists
        try:
            Path(self.temporary_folder).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
            
        
        ## bucket address and file path
        s = self.products_data_address
        bucket = s[s.find('s3://')+len('s3://'):s.rfind('/')]
        file_path = s[s.rfind('/')+1:]
        local_file_path = f'./{self.temporary_folder}/{self.temporary_file}'
        
        # download the file to the temporary folder
        try:
            s3 = boto3.client('s3')
            s3.download_file(bucket, file_path, local_file_path)
            s3.close()
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        
        return local_file_path

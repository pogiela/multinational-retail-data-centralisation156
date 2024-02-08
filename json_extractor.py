import sys
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import requests

class JsonDataExtractor:
    def __init__(self):
        # initiate local variables
        load_dotenv()
        self.date_events_data_address = os.getenv('DATE_EVENTS_DATA')
        self.temporary_folder = './temp_files'
        self.temporary_file = 'date_events.json'


    def extract_from_s3(self):
        print('\n############## Accessing the file: ##############') 
        print(f'\n--> Data link: {self.date_events_data_address}\n\n')
        
        if self.date_events_data_address:
            print('\n############## Checking the file: ##############') 
            # download the pdf file to a temporary folder
            downloaded_file = self.__download_file(self.date_events_data_address)
            
            print(f"\n-->File accessed successfully.\n\n")
            print('\n############## Processing the file: ##############\n\n') 
            final_df = pd.read_json(downloaded_file)
            print(f'\n--> Data loaded successfully\n\n')
            print('\n############## First 5 rows of data: ##############\n') 
            print(final_df.head())
            print('\n\n############## Data information: ##############\n')
            print(final_df.info())
            
            return final_df
            
    def __download_file(self, file_path):
        # Create a temporary folder if not exists
        try:
            Path(self.temporary_folder).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
            
        # download the file to the temporary folder
        try:
            with requests.get(file_path, stream=True) as response:
                response.raise_for_status()  # Raise an exception for error status codes
                try:
                    with open(self.temporary_folder + '/' + self.temporary_file, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                except Exception as e:
                    print(f'Error occured: {e}')
                    sys.exit()
        except Exception as e:
            print(f'Error occured: {e}')
            sys.exit()
        
        return self.temporary_folder + '/' + self.temporary_file
    


import sys
import os
from dotenv import load_dotenv
import requests
import pandas as pd

class ApiDataExtractor:
    def __init__(self):
        # initiate local variables
        load_dotenv()
        self.headers = self.__create_headers()
        self.number_of_stores_api = os.getenv('number_of_stores_api')
        self.retrive_store_api = os.getenv('retrive_store_api')
        
    def retrive_stores_data(self):
        print('\n############## Accessing the API: ##############')
        number_of_stores = self.__list_number_of_stores()
        print(f"\n--> There are {number_of_stores} stores available in the API\n\n")
        print('\n############## Downloading stores data from the API: ##############\n\n') 
        stores_data = [] # initiate blank array
        for store_number in range(number_of_stores):
            try:
                self.__progress(store_number + 1, number_of_stores)
                store_data = self.__retrieve_store_data(store_number)
                stores_data.extend([store_data])
            except Exception as e:
                print(f'Error occured when processing store no. {store_number}: {e}')
                sys.exit()
        
        # create pandas dataframe and return it
        df = pd.DataFrame(stores_data)
        df = df.set_index('index')  # Set 'index' column as the DataFrame index
        print("\n\nAll data retrieved from the API.")
        return df
        
    def __list_number_of_stores(self):
        # create a get request to the api endpoint
        response = requests.get(self.number_of_stores_api, headers=self.headers)
        if response.status_code == 200:
            # Access the response data as JSON
            data = response.json()
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")

        return data['number_stores']
    
    
    def __retrieve_store_data(self, store_number):
        # create a get request to the api endpoint
        url = f"{self.retrive_store_api}{store_number}" 
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            # Access the response data as JSON
            data = response.json()
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")

        return data
    
    
    def __create_headers(self):
        x_api_key = os.getenv('x_api_key')
        
        return { 
            "Content-Type": "application/json",
            "x-api-key": x_api_key
        }
    
    
    def __progress(self, count, total):
        bar_len = 60
        filled_len = int(round(bar_len * count / float(total)))

        percents = round(100.0 * count / float(total), 1)
        bar = '#' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.write(f'[{bar}] {percents}%  [{count} / {total}]\r')
        sys.stdout.flush()
        
        
        
# api_extractor = ApiDataExtractor()
# output_data = api_extractor.retrive_stores_data()
# output_data.to_excel('./api_data.xlsx')